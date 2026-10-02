using NexusControl.Core;
using NUnit.Framework;
namespace NexusControl.Tests;
public class AnomalyDetectorTests
{
    readonly AnomalyDetector _d = new();
    static SupplyEvent Ev(string k, double v, double b = 100) => new(Guid.NewGuid(), 1, k, "SKU-1", v, b, DateTime.UtcNow);

    [Test] public void ZeroInventory_IsCriticalStockout() { var a = _d.Detect(Ev("inventory.level", 0)); Assert.That(a!.Type, Is.EqualTo("Stockout")); Assert.That(a.Severity, Is.EqualTo(4)); }
    [Test] public void LowInventory_IsLowStock() => Assert.That(_d.Detect(Ev("inventory.level", 15))!.Type, Is.EqualTo("LowStock"));
    [Test] public void ShipmentDelay72h_EscalatesToSeverity4() => Assert.That(_d.Detect(Ev("shipment.delay", 80))!.Severity, Is.EqualTo(4));
    [Test] public void SmallDelay_NoIncident() => Assert.That(_d.Detect(Ev("shipment.delay", 2, 2)), Is.Null);
    [Test] public void LowSupplierScore_RaisesSupplierRisk() => Assert.That(_d.Detect(Ev("supplier.score", 40))!.Type, Is.EqualTo("SupplierRisk"));
    [Test] public void ZScoreOutlier_RaisesMetricAnomaly() => Assert.That(_d.Detect(Ev("order.rate", 500, 100), 50)!.Type, Is.EqualTo("MetricAnomaly"));
}
