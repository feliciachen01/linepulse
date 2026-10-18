using System.Text.Json;
using System.Text.Json.Nodes;

namespace LinePulse.Api;

/// <summary>
/// Recorded investigations written by scripts/investigate_alerts.py (LLM agent or the
/// deterministic investigator), plus the diagnosis-accuracy evaluation and supervisor reviews.
/// Like handover notes, these are produced outside Databricks and served from files in both modes.
/// The records are passed through as JSON: the dashboard replays them step by step.
/// </summary>
public sealed class InvestigationFiles(string dir)
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private string RecordsPath => Path.Combine(dir, "gold_investigations.json");
    private string EvalPath => Path.Combine(dir, "gold_investigation_eval.json");
    private string ReviewsPath => Path.Combine(dir, "investigation_reviews.json");

    public static readonly string[] Causes = ["burner_fault", "thermocouple_drift", "belt_slowdown", "filler_drift", "unclear"];

    private static async Task<JsonNode?> Load(string path, CancellationToken ct)
    {
        if (!File.Exists(path)) return null;
        await using var fs = File.OpenRead(path);
        return await JsonNode.ParseAsync(fs, cancellationToken: ct);
    }

    private async Task<JsonObject> Reviews(CancellationToken ct) => (await Load(ReviewsPath, ct))?.AsObject() ?? [];

    /// <summary>One line per alert: enough for the alerts table, without the recorded steps.</summary>
    public async Task<IReadOnlyList<JsonObject>> ListAsync(CancellationToken ct)
    {
        var records = (await Load(RecordsPath, ct))?.AsArray() ?? [];
        var reviews = await Reviews(ct);
        return records.Select(r => r!.AsObject()).Select(r => new JsonObject
        {
            ["alert_id"] = r["alert_id"]?.DeepClone(),
            ["line_id"] = r["line_id"]?.DeepClone(),
            ["signal"] = r["signal"]?.DeepClone(),
            ["start_minute"] = r["start_minute"]?.DeepClone(),
            ["writer"] = r["writer"]?.DeepClone(),
            ["model"] = r["model"]?.DeepClone(),
            ["root_cause"] = r["diagnosis"]?["root_cause"]?.DeepClone(),
            ["confidence"] = r["diagnosis"]?["confidence"]?.DeepClone(),
            ["status"] = r["status"]?.DeepClone(),
            ["review"] = reviews[r["alert_id"]!.GetValue<string>()]?.DeepClone(),
        }).ToList();
    }

    public async Task<JsonObject?> GetAsync(string alertId, CancellationToken ct)
    {
        var records = (await Load(RecordsPath, ct))?.AsArray() ?? [];
        var rec = records.FirstOrDefault(r => r?["alert_id"]?.GetValue<string>() == alertId)?.DeepClone().AsObject();
        if (rec is null) return null;
        rec["review"] = (await Reviews(ct))[alertId]?.DeepClone();
        return rec;
    }

    public Task<JsonNode?> EvaluationAsync(CancellationToken ct) => Load(EvalPath, ct);

    /// <summary>Accept or override a diagnosis. Latest review per alert wins.</summary>
    public async Task<JsonObject> SaveReviewAsync(string alertId, ReviewRequest req, string diagnosed, CancellationToken ct)
    {
        var review = new JsonObject
        {
            ["decision"] = req.Decision,
            ["root_cause"] = req.Decision == "accept" ? diagnosed : req.RootCause,
            ["note"] = req.Note,
            ["at"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"),
        };
        await _lock.WaitAsync(ct);
        try
        {
            var all = await Reviews(ct);
            all[alertId] = review.DeepClone();
            await File.WriteAllTextAsync(ReviewsPath, all.ToJsonString(new JsonSerializerOptions { WriteIndented = true }), ct);
        }
        finally { _lock.Release(); }
        return review;
    }
}

public sealed record ReviewRequest(
    [property: System.Text.Json.Serialization.JsonPropertyName("decision")] string Decision,
    [property: System.Text.Json.Serialization.JsonPropertyName("root_cause")] string? RootCause,
    [property: System.Text.Json.Serialization.JsonPropertyName("note")] string? Note)
{
    public string? Problem() => Decision switch
    {
        "accept" => Note is { Length: > 500 } ? "note is at most 500 characters" : null,
        "override" when RootCause is null || !InvestigationFiles.Causes.Contains(RootCause)
            => $"override needs root_cause, one of {string.Join(", ", InvestigationFiles.Causes)}",
        "override" => Note is { Length: > 500 } ? "note is at most 500 characters" : null,
        _ => "decision is accept or override",
    };
}
