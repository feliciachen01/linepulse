using System.Text.RegularExpressions;
using LinePulse.Api;
using Microsoft.Extensions.Caching.Memory;

var builder = WebApplication.CreateBuilder(args);

// Mode comes from config or env: LINEPULSE__MODE=Local|Databricks
var mode = builder.Configuration["LinePulse:Mode"] ?? "Local";
builder.Services.AddMemoryCache();
builder.Services.AddProblemDetails();

if (mode.Equals("Databricks", StringComparison.OrdinalIgnoreCase))
{
    // Secrets come from environment variables, never from appsettings.json.
    var opt = new DatabricksOptions
    {
        Host = Environment.GetEnvironmentVariable("DATABRICKS_HOST") ?? "",
        Token = Environment.GetEnvironmentVariable("DATABRICKS_TOKEN") ?? "",
        WarehouseId = Environment.GetEnvironmentVariable("DATABRICKS_WAREHOUSE_ID") ?? "",
    };
    builder.Configuration.GetSection("LinePulse:Databricks").Bind(opt);
    if (string.IsNullOrWhiteSpace(opt.Host) || string.IsNullOrWhiteSpace(opt.Token) || string.IsNullOrWhiteSpace(opt.WarehouseId))
        throw new InvalidOperationException("Set DATABRICKS_HOST, DATABRICKS_TOKEN and DATABRICKS_WAREHOUSE_ID for Databricks mode.");
    builder.Services.AddSingleton(opt);
    builder.Services.AddHttpClient<IGoldStore, DatabricksSqlGoldStore>(opt.ConfigureClient);
}
else
{
    var dir = builder.Configuration["LinePulse:LocalExportDir"] ?? "../../local_data/export";
    var full = Path.GetFullPath(Path.Combine(builder.Environment.ContentRootPath, dir));
    builder.Services.AddSingleton<IGoldStore>(new LocalJsonGoldStore(full));
}

var handoverPath = builder.Configuration["LinePulse:HandoverFile"] ?? "../../local_data/export/gold_handover.json";
builder.Services.AddSingleton(new HandoverFile(
    Path.GetFullPath(Path.Combine(builder.Environment.ContentRootPath, handoverPath))));
var investigationsDir = builder.Configuration["LinePulse:InvestigationsDir"] ?? "../../local_data/export";
builder.Services.AddSingleton(new InvestigationFiles(
    Path.GetFullPath(Path.Combine(builder.Environment.ContentRootPath, investigationsDir))));

var app = builder.Build();
app.UseExceptionHandler(e => e.Run(async ctx =>
{
    var ex = ctx.Features.Get<Microsoft.AspNetCore.Diagnostics.IExceptionHandlerFeature>()?.Error;
    // No gold data yet is an expected state, not a server bug: say how to fix it.
    var (code, title) = ex switch
    {
        FileNotFoundException => (503, "Gold data not found. Run: python scripts/run_local.py"),
        HttpRequestException or TaskCanceledException => (502, "Databricks SQL warehouse unreachable or slow (it may be starting)."),
        InvalidOperationException ioe when ioe.Message.StartsWith("Databricks") || ioe.Message.StartsWith("Statement")
            => (502, ioe.Message),
        _ => (500, "Unexpected error"),
    };
    ctx.Response.StatusCode = code;
    await Results.Problem(title: title, statusCode: code).ExecuteAsync(ctx);
}));
app.UseDefaultFiles();
app.UseStaticFiles();

var lineIdPattern = new Regex("^L[0-9]{1,3}$", RegexOptions.Compiled);
var alertIdPattern = new Regex("^L[0-9]{1,3}-[a-z0-9_]{1,40}-[0-9]{8}T[0-9]{4}$", RegexOptions.Compiled);
string[] signals = ["oven_z1_c", "oven_z2_c", "oven_z3_c", "fill_weight_g", "belt_speed_mpm", "proofer_rh_pct"];
string[] alertStatuses = ["open", "closed"];

app.MapGet("/health", (IGoldStore store) => Results.Ok(new { status = "ok", mode = store.Mode }));

var api = app.MapGroup("/api");

api.MapGet("/lines", async (IGoldStore s, CancellationToken ct) => Results.Ok(await s.GetLinesAsync(ct)));

api.MapGet("/lines/{lineId}/status", async (string lineId, IGoldStore s, CancellationToken ct) =>
{
    if (!lineIdPattern.IsMatch(lineId)) return Results.BadRequest(new { error = "lineId must look like L1" });
    var line = (await s.GetLinesAsync(ct)).FirstOrDefault(l => l.LineId == lineId);
    return line is null ? Results.NotFound() : Results.Ok(line);
});

api.MapGet("/alerts", async (string? lineId, string? status, int? limit, IGoldStore s, CancellationToken ct) =>
{
    if (lineId is not null && !lineIdPattern.IsMatch(lineId)) return Results.BadRequest(new { error = "bad lineId" });
    if (status is not null && !alertStatuses.Contains(status)) return Results.BadRequest(new { error = "status is open|closed" });
    var n = Math.Clamp(limit ?? 50, 1, 500);
    return Results.Ok(await s.GetAlertsAsync(lineId, status, n, ct));
});

api.MapGet("/shifts", async (string? lineId, IGoldStore s, CancellationToken ct) =>
{
    if (lineId is not null && !lineIdPattern.IsMatch(lineId)) return Results.BadRequest(new { error = "bad lineId" });
    return Results.Ok(await s.GetShiftKpisAsync(lineId, ct));
});

api.MapGet("/signals/{lineId}/{signal}", async (string lineId, string signal, int? minutes, IGoldStore s, CancellationToken ct) =>
{
    if (!lineIdPattern.IsMatch(lineId) || !signals.Contains(signal))
        return Results.BadRequest(new { error = $"signal must be one of {string.Join(", ", signals)}" });
    return Results.Ok(await s.GetSignalAsync(lineId, signal, Math.Clamp(minutes ?? 180, 10, 1440), ct));
});

api.MapGet("/handover", async (HandoverFile f, CancellationToken ct) => Results.Ok(await f.ReadAsync(ct)));

api.MapGet("/investigations", async (InvestigationFiles f, CancellationToken ct) => Results.Ok(await f.ListAsync(ct)));

api.MapGet("/investigations/evaluation", async (InvestigationFiles f, CancellationToken ct) =>
    await f.EvaluationAsync(ct) is { } e ? Results.Ok(e)
        : Results.NotFound(new { error = "No evaluation yet. Run: python scripts/evaluate_investigations.py" }));

api.MapGet("/investigations/{alertId}", async (string alertId, InvestigationFiles f, CancellationToken ct) =>
{
    if (!alertIdPattern.IsMatch(alertId)) return Results.BadRequest(new { error = "alertId must look like L1-oven_z3_c-20261019T0809" });
    return await f.GetAsync(alertId, ct) is { } rec ? Results.Ok(rec) : Results.NotFound();
});

// A supervisor accepts the diagnosis or overrides it with the cause they found. Kept next to the
// recorded investigation so agreement can be measured later.
api.MapPost("/investigations/{alertId}/review", async (string alertId, ReviewRequest req, InvestigationFiles f, CancellationToken ct) =>
{
    if (!alertIdPattern.IsMatch(alertId)) return Results.BadRequest(new { error = "bad alertId" });
    if (req.Problem() is { } problem) return Results.BadRequest(new { error = problem });
    var rec = await f.GetAsync(alertId, ct);
    if (rec is null) return Results.NotFound();
    var diagnosed = rec["diagnosis"]?["root_cause"]?.GetValue<string>() ?? "unclear";
    return Results.Ok(await f.SaveReviewAsync(alertId, req, diagnosed, ct));
});

api.MapGet("/data-quality", async (IGoldStore s, CancellationToken ct) => Results.Ok(await s.GetDataQualityAsync(ct)));

app.Run();

public partial class Program;
