namespace NexusControl.Core;
public record SupplyEvent(Guid EventId, int TenantId, string Kind, string EntityKey, double Value, double Baseline, DateTime At);
public record Anomaly(string Type, string EntityKey, byte Severity, int SlaMinutes);

public class AnomalyDetector
{
    public const double ZThreshold = 3.0;
    // Rules: shipment delay, stockout, supplier risk; fallback z-score on any metric.
    public Anomaly? Detect(SupplyEvent e, double stdDev = 1)
    {
        switch (e.Kind)
        {
            case "shipment.delay" when e.Value >= 24: return new("ShipmentDelay", e.EntityKey, (byte)(e.Value >= 72 ? 4 : 3), 120);
            case "inventory.level" when e.Value <= 0: return new("Stockout", e.EntityKey, 4, 60);
            case "inventory.level" when e.Value < e.Baseline * 0.2: return new("LowStock", e.EntityKey, 2, 240);
            case "supplier.score" when e.Value < 60: return new("SupplierRisk", e.EntityKey, 3, 480);
        }
        if (stdDev > 0 && Math.Abs(e.Value - e.Baseline) / stdDev > ZThreshold) return new("MetricAnomaly", e.EntityKey, 2, 240);
        return null;
    }
}
