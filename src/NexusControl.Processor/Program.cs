using Confluent.Kafka;
using Dapper;
using Microsoft.Data.SqlClient;
using Nest;
using NexusControl.Core;
using StackExchange.Redis;
using System.Diagnostics;
using System.Text.Json;

var host = Host.CreateApplicationBuilder(args);
host.Services.AddHostedService<EventProcessor>();
host.Build().Run();

public class EventProcessor(ILogger<EventProcessor> log, IConfiguration cfg) : BackgroundService
{
    const int MaxRetries = 3;
    readonly AnomalyDetector _detector = new();
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var servers = cfg["Kafka:Servers"] ?? "localhost:9092";
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig {
            BootstrapServers = servers, GroupId = "nexus-processor", EnableAutoCommit = false, AutoOffsetReset = AutoOffsetReset.Earliest }).Build();
        using var dlq = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = servers }).Build();
        var redis = (await ConnectionMultiplexer.ConnectAsync(cfg["Redis"] ?? "localhost:6379")).GetDatabase();
        var es = new ElasticClient(new ConnectionSettings(new Uri(cfg["Elastic"] ?? "http://localhost:9200")).DefaultIndex("sc-events"));
        var sql = cfg.GetConnectionString("Sql") ?? "Server=localhost;Database=NexusControl;User Id=sa;Password=Your_pass123;TrustServerCertificate=True";
        consumer.Subscribe("sc.events");

        while (!ct.IsCancellationRequested)
        {
            var cr = consumer.Consume(ct);
            var sw = Stopwatch.StartNew();
            for (int attempt = 1; ; attempt++)
            {
                try { await Handle(cr.Message.Value, redis, es, sql); break; }
                catch (Exception ex) when (attempt < MaxRetries)
                { log.LogWarning(ex, "Retry {Attempt} offset {Offset}", attempt, cr.Offset); await Task.Delay(200 * (int)Math.Pow(2, attempt), ct); }
                catch (Exception ex)
                {   // poison message -> dead-letter queue, never block the partition
                    log.LogError(ex, "Sending to DLQ offset {Offset}", cr.Offset);
                    await dlq.ProduceAsync("sc.events.dlq", cr.Message, ct); break;
                }
            }
            consumer.Commit(cr);
            log.LogInformation("event_processed latency_ms={Ms}", sw.ElapsedMilliseconds); // scraped by Telegraf/Grafana
        }
    }

    async Task Handle(string json, IDatabase redis, ElasticClient es, string sql)
    {
        var e = JsonSerializer.Deserialize<SupplyEvent>(json)!;
        if (!await redis.StringSetAsync($"seen:{e.EventId}", 1, TimeSpan.FromHours(24), When.NotExists)) return; // idempotency
        await es.IndexDocumentAsync(e);
        var a = _detector.Detect(e);
        if (a is null) return;
        // Alert correlation: burst of alerts on the same entity within 5 min = one incident, escalating
        await redis.StringIncrementAsync($"corr:{e.TenantId}:{a.Type}:{a.EntityKey}");
        await redis.KeyExpireAsync($"corr:{e.TenantId}:{a.Type}:{a.EntityKey}", TimeSpan.FromMinutes(5));
        await using var c = new SqlConnection(sql);
        await c.ExecuteAsync("dbo.sp_UpsertIncident", new { e.TenantId, a.Type, a.EntityKey, Severity = a.Severity, a.SlaMinutes }, commandType: System.Data.CommandType.StoredProcedure);
    }
}
