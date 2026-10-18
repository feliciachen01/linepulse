using System.Text.Json.Serialization;

namespace LinePulse.Api;

// Column names match the gold tables exactly, so the same records deserialize
// from the local JSON export and from Databricks SQL results.

public sealed record LineStatus(
    [property: JsonPropertyName("line_id")] string LineId,
    [property: JsonPropertyName("sku")] string Sku,
    [property: JsonPropertyName("product")] string Product,
    [property: JsonPropertyName("as_of")] DateTime? AsOf,
    [property: JsonPropertyName("oven_z1_c")] double? OvenZ1C,
    [property: JsonPropertyName("oven_z2_c")] double? OvenZ2C,
    [property: JsonPropertyName("oven_z3_c")] double? OvenZ3C,
    [property: JsonPropertyName("fill_weight_g")] double? FillWeightG,
    [property: JsonPropertyName("scrap_rate_15m_pct")] double? ScrapRate15mPct,
    [property: JsonPropertyName("current_lot")] string? CurrentLot,
    [property: JsonPropertyName("open_alerts")] long OpenAlerts,
    [property: JsonPropertyName("open_alert_ids")] List<string>? OpenAlertIds,
    [property: JsonPropertyName("status")] string Status);

public sealed record Alert(
    [property: JsonPropertyName("alert_id")] string AlertId,
    [property: JsonPropertyName("line_id")] string LineId,
    [property: JsonPropertyName("signal")] string Signal,
    [property: JsonPropertyName("direction")] string Direction,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("start_minute")] DateTime StartMinute,
    [property: JsonPropertyName("detected_at")] DateTime DetectedAt,
    [property: JsonPropertyName("end_minute")] DateTime EndMinute,
    [property: JsonPropertyName("duration_min")] int DurationMin,
    [property: JsonPropertyName("peak_abs_dev")] double PeakAbsDev,
    [property: JsonPropertyName("threshold")] double Threshold,
    [property: JsonPropertyName("affected_lots")] List<string> AffectedLots,
    [property: JsonPropertyName("units_in_window")] long UnitsInWindow,
    [property: JsonPropertyName("rejects_in_window")] long RejectsInWindow);

public sealed record ShiftKpi(
    [property: JsonPropertyName("line_id")] string LineId,
    [property: JsonPropertyName("shift_date")] string ShiftDate,
    [property: JsonPropertyName("shift")] string Shift,
    [property: JsonPropertyName("units")] long Units,
    [property: JsonPropertyName("rejects")] long Rejects,
    [property: JsonPropertyName("scrap_rate_pct")] double ScrapRatePct,
    [property: JsonPropertyName("rejects_underweight")] long RejectsUnderweight,
    [property: JsonPropertyName("rejects_bake_defect")] long RejectsBakeDefect,
    [property: JsonPropertyName("avg_accepted_weight_g")] double AvgAcceptedWeightG,
    [property: JsonPropertyName("giveaway_g_per_pack")] double GiveawayGPerPack,
    [property: JsonPropertyName("giveaway_kg")] double GiveawayKg,
    [property: JsonPropertyName("giveaway_cost_cad")] double GiveawayCostCad,
    [property: JsonPropertyName("alerts")] long Alerts);

public sealed record SignalMinute(
    [property: JsonPropertyName("line_id")] string LineId,
    [property: JsonPropertyName("signal")] string Signal,
    [property: JsonPropertyName("minute")] DateTime Minute,
    [property: JsonPropertyName("mean_value")] double MeanValue,
    [property: JsonPropertyName("setpoint")] double Setpoint,
    [property: JsonPropertyName("ewma_dev")] double EwmaDev,
    [property: JsonPropertyName("threshold")] double Threshold,
    [property: JsonPropertyName("over")] bool Over);

public sealed record DqSummary(
    [property: JsonPropertyName("event_type")] string EventType,
    [property: JsonPropertyName("rule")] string Rule,
    [property: JsonPropertyName("records")] long Records);
