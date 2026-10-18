using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Caching.Memory;

namespace LinePulse.Api;

/// <summary>Read-only access to the gold tables. The API never touches bronze or silver.</summary>
public interface IGoldStore
{
    string Mode { get; }
    Task<IReadOnlyList<LineStatus>> GetLinesAsync(CancellationToken ct);
    Task<IReadOnlyList<Alert>> GetAlertsAsync(string? lineId, string? status, int limit, CancellationToken ct);
    Task<IReadOnlyList<ShiftKpi>> GetShiftKpisAsync(string? lineId, CancellationToken ct);
    Task<IReadOnlyList<SignalMinute>> GetSignalAsync(string lineId, string signal, int minutes, CancellationToken ct);
    Task<IReadOnlyList<DqSummary>> GetDataQualityAsync(CancellationToken ct);
}

public static class Json
{
    public static readonly JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };
}

/// <summary>
/// Handover notes are written outside Databricks (Free Edition cannot call LLM APIs) by
/// scripts/generate_handover.py into a JSON file; both API modes serve that file.
/// A missing file means "not generated yet": an empty list, not an error.
/// </summary>
public sealed class HandoverFile(string path)
{
    public async Task<IReadOnlyList<HandoverNote>> ReadAsync(CancellationToken ct)
    {
        if (!File.Exists(path)) return [];
        await using var fs = File.OpenRead(path);
        var notes = await JsonSerializer.DeserializeAsync<List<HandoverNote>>(fs, Json.Options, ct) ?? [];
        return notes.OrderByDescending(n => n.ShiftDate).ThenByDescending(n => n.Shift).ToList();
    }
}

/// <summary>Local mode: reads the JSON files that scripts/run_local.py exports. No cloud needed.</summary>
public sealed class LocalJsonGoldStore(string exportDir) : IGoldStore
{
    public string Mode => "local";

    private async Task<List<T>> Load<T>(string table, CancellationToken ct)
    {
        var path = Path.Combine(exportDir, $"{table}.json");
        if (!File.Exists(path))
            throw new FileNotFoundException($"{path} not found. Run: python scripts/run_local.py", path);
        await using var fs = File.OpenRead(path);
        return await JsonSerializer.DeserializeAsync<List<T>>(fs, Json.Options, ct) ?? [];
    }

    public async Task<IReadOnlyList<LineStatus>> GetLinesAsync(CancellationToken ct) =>
        (await Load<LineStatus>("gold_line_status", ct)).OrderBy(l => l.LineId).ToList();

    public async Task<IReadOnlyList<Alert>> GetAlertsAsync(string? lineId, string? status, int limit, CancellationToken ct) =>
        (await Load<Alert>("gold_alerts", ct))
            .Where(a => lineId is null || a.LineId == lineId)
            .Where(a => status is null || a.Status == status)
            .OrderByDescending(a => a.StartMinute).Take(limit).ToList();

    public async Task<IReadOnlyList<ShiftKpi>> GetShiftKpisAsync(string? lineId, CancellationToken ct) =>
        (await Load<ShiftKpi>("gold_shift_kpis", ct))
            .Where(k => lineId is null || k.LineId == lineId)
            .OrderByDescending(k => k.ShiftDate).ThenBy(k => k.LineId).ThenBy(k => k.Shift).ToList();

    public async Task<IReadOnlyList<SignalMinute>> GetSignalAsync(string lineId, string signal, int minutes, CancellationToken ct)
    {
        var rows = (await Load<SignalMinute>("gold_signal_minute", ct))
            .Where(s => s.LineId == lineId && s.Signal == signal).OrderBy(s => s.Minute).ToList();
        return rows.Skip(Math.Max(0, rows.Count - minutes)).ToList();
    }

    public async Task<IReadOnlyList<DqSummary>> GetDataQualityAsync(CancellationToken ct) =>
        await Load<DqSummary>("gold_dq_summary", ct);
}

/// <summary>
/// Databricks mode: runs parameterized SQL on a SQL warehouse through the Statement Execution API
/// (POST /api/2.0/sql/statements). Results are cached briefly: gold only changes when the
/// pipeline runs, and every cache hit is a query the Free Edition warehouse does not have to run.
/// </summary>
public sealed class DatabricksSqlGoldStore(HttpClient http, IMemoryCache cache, DatabricksOptions opt,
                                           ILogger<DatabricksSqlGoldStore> log) : IGoldStore
{
    public string Mode => "databricks";
    private string T(string table) => $"{opt.Catalog}.{opt.Schema}.{table}";

    public Task<IReadOnlyList<LineStatus>> GetLinesAsync(CancellationToken ct) =>
        Query<LineStatus>($"SELECT * FROM {T("gold_line_status")} ORDER BY line_id", [], ct);

    public Task<IReadOnlyList<Alert>> GetAlertsAsync(string? lineId, string? status, int limit, CancellationToken ct) =>
        Query<Alert>($"""
            SELECT * FROM {T("gold_alerts")}
            WHERE (:line_id IS NULL OR line_id = :line_id) AND (:status IS NULL OR status = :status)
            ORDER BY start_minute DESC LIMIT {limit}
            """, [("line_id", lineId), ("status", status)], ct);

    public Task<IReadOnlyList<ShiftKpi>> GetShiftKpisAsync(string? lineId, CancellationToken ct) =>
        Query<ShiftKpi>($"""
            SELECT * FROM {T("gold_shift_kpis")} WHERE (:line_id IS NULL OR line_id = :line_id)
            ORDER BY shift_date DESC, line_id, shift
            """, [("line_id", lineId)], ct);

    public Task<IReadOnlyList<SignalMinute>> GetSignalAsync(string lineId, string signal, int minutes, CancellationToken ct) =>
        Query<SignalMinute>($"""
            SELECT * FROM (
              SELECT * FROM {T("gold_signal_minute")} WHERE line_id = :line_id AND signal = :signal
              ORDER BY minute DESC LIMIT {minutes}) ORDER BY minute
            """, [("line_id", lineId), ("signal", signal)], ct);

    public Task<IReadOnlyList<DqSummary>> GetDataQualityAsync(CancellationToken ct) =>
        Query<DqSummary>($"SELECT * FROM {T("gold_dq_summary")} ORDER BY records DESC", [], ct);

    private async Task<IReadOnlyList<T>> Query<T>(string sql, (string Name, string? Value)[] parameters, CancellationToken ct)
    {
        var key = sql + "|" + string.Join("|", parameters.Select(p => $"{p.Name}={p.Value}"));
        if (cache.TryGetValue(key, out IReadOnlyList<T>? hit) && hit is not null) return hit;

        var body = new JsonObject
        {
            ["warehouse_id"] = opt.WarehouseId,
            ["statement"] = sql,
            ["wait_timeout"] = "30s",
            ["on_wait_timeout"] = "CANCEL",
            ["format"] = "JSON_ARRAY",
            ["disposition"] = "INLINE",
            ["parameters"] = new JsonArray(parameters.Select(p =>
                (JsonNode)new JsonObject { ["name"] = p.Name, ["value"] = p.Value, ["type"] = "STRING" }).ToArray()),
        };
        var started = DateTime.UtcNow;
        using var resp = await http.PostAsync("/api/2.0/sql/statements/",
            new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json"), ct);
        var text = await resp.Content.ReadAsStringAsync(ct);
        if (!resp.IsSuccessStatusCode)
            throw new InvalidOperationException($"Databricks returned {(int)resp.StatusCode}: {text}");

        var result = JsonNode.Parse(text)!.AsObject();
        var state = result["status"]?["state"]?.GetValue<string>();
        if (state != "SUCCEEDED")
            throw new InvalidOperationException($"Statement {state}: {result["status"]?["error"]?["message"]}");

        var rows = StatementResultParser.ToRows(result);
        // Large results arrive in chunks; gold tables are small, but follow the links anyway.
        var next = result["result"]?["next_chunk_internal_link"]?.GetValue<string>();
        while (next is not null)
        {
            var chunk = JsonNode.Parse(await http.GetStringAsync(next, ct))!.AsObject();
            rows.AddRange(StatementResultParser.ToRows(result["manifest"]!, chunk["data_array"]));
            next = chunk["next_chunk_internal_link"]?.GetValue<string>();
        }
        log.LogInformation("SQL {Rows} rows in {Ms} ms", rows.Count, (DateTime.UtcNow - started).TotalMilliseconds);

        var typed = rows.Select(r => r.Deserialize<T>(Json.Options)!).ToList();
        cache.Set(key, (IReadOnlyList<T>)typed, TimeSpan.FromSeconds(opt.CacheSeconds));
        return typed;
    }
}

/// <summary>Turns a JSON_ARRAY statement result (all values as strings) into typed JSON objects.</summary>
public static class StatementResultParser
{
    public static List<JsonObject> ToRows(JsonObject response) =>
        ToRows(response["manifest"]!, response["result"]?["data_array"]);

    public static List<JsonObject> ToRows(JsonNode manifest, JsonNode? dataArray)
    {
        var columns = manifest["schema"]!["columns"]!.AsArray()
            .Select(c => (Name: c!["name"]!.GetValue<string>(), Type: c["type_name"]!.GetValue<string>())).ToList();
        var rows = new List<JsonObject>();
        foreach (var row in dataArray?.AsArray() ?? [])
        {
            var obj = new JsonObject();
            var cells = row!.AsArray();
            for (var i = 0; i < columns.Count; i++)
                obj[columns[i].Name] = Convert(cells[i]?.GetValue<string>(), columns[i].Type);
            rows.Add(obj);
        }
        return rows;
    }

    public static JsonNode? Convert(string? raw, string typeName) => raw is null ? null : typeName switch
    {
        "BOOLEAN" => JsonValue.Create(bool.Parse(raw)),
        "BYTE" or "SHORT" or "INT" or "LONG" => JsonValue.Create(long.Parse(raw)),
        "FLOAT" or "DOUBLE" or "DECIMAL" => JsonValue.Create(double.Parse(raw, System.Globalization.CultureInfo.InvariantCulture)),
        "ARRAY" or "MAP" or "STRUCT" => JsonNode.Parse(raw),
        _ => JsonValue.Create(raw), // STRING, DATE, TIMESTAMP stay strings; System.Text.Json parses ISO dates
    };
}

public sealed class DatabricksOptions
{
    public string Host { get; set; } = "";
    public string Token { get; set; } = "";
    public string WarehouseId { get; set; } = "";
    public string Catalog { get; set; } = "workspace";
    public string Schema { get; set; } = "linepulse";
    public int CacheSeconds { get; set; } = 60;

    public void ConfigureClient(HttpClient http)
    {
        http.BaseAddress = new Uri(Host.StartsWith("http") ? Host : $"https://{Host}");
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Token);
        http.Timeout = TimeSpan.FromSeconds(45);
    }
}
