using System.Security.Claims;
using System.Text.Json;
using Confluent.Kafka;
using Dapper;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Data.SqlClient;
using Microsoft.IdentityModel.Tokens;
using NexusControl.Core;

var b = WebApplication.CreateBuilder(args);
var cfg = b.Configuration;
b.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme).AddJwtBearer(o =>
{   // OAuth2 / OIDC: point Authority at Azure AD / Entra ID in AKS; dev uses a symmetric key
    o.Authority = cfg["Auth:Authority"];
    o.Audience = "nexuscontrol-api";
    if (string.IsNullOrEmpty(o.Authority))
        o.TokenValidationParameters = new() { ValidateIssuer = false, ValidateAudience = false,
            IssuerSigningKey = new SymmetricSecurityKey(System.Text.Encoding.UTF8.GetBytes(cfg["Auth:DevKey"] ?? "dev-key-change-me-0123456789abcdef")) };
});
b.Services.AddAuthorizationBuilder()
    .AddPolicy("Operator", p => p.RequireRole("operator", "admin"))
    .AddPolicy("Producer", p => p.RequireRole("producer", "admin"));
b.Services.AddHealthChecks();
b.Services.AddCors(o => o.AddDefaultPolicy(p => p.AllowAnyHeader().AllowAnyMethod().WithOrigins("http://localhost:5173")));
b.Services.AddSingleton(_ => new ProducerBuilder<string, string>(new ProducerConfig {
    BootstrapServers = cfg["Kafka:Servers"] ?? "localhost:9092", EnableIdempotence = true, Acks = Acks.All }).Build());

var app = b.Build();
app.UseCors(); app.UseAuthentication(); app.UseAuthorization();
app.MapHealthChecks("/health/live"); app.MapHealthChecks("/health/ready");

int Tenant(ClaimsPrincipal u) => int.Parse(u.FindFirstValue("tid") ?? throw new UnauthorizedAccessException());
string Conn() => cfg.GetConnectionString("Sql") ?? "Server=localhost;Database=NexusControl;User Id=sa;Password=Your_pass123;TrustServerCertificate=True";

var v1 = app.MapGroup("/api/v1").RequireAuthorization();

v1.MapPost("/events", async (SupplyEvent[] events, ClaimsPrincipal u, IProducer<string, string> kafka) =>
{
    var tid = Tenant(u);
    if (events.Length is 0 or > 1000) return Results.BadRequest("batch size must be 1..1000");
    foreach (var e in events)   // tenant comes from the token, never from the payload; key = entity => per-entity ordering
        await kafka.ProduceAsync("sc.events", new() { Key = $"{tid}:{e.EntityKey}", Value = JsonSerializer.Serialize(e with { TenantId = tid }) });
    return Results.Accepted(value: new { accepted = events.Length });
}).RequireAuthorization("Producer");

v1.MapGet("/incidents", async (ClaimsPrincipal u) =>
{
    await using var c = new SqlConnection(Conn());
    return Results.Ok(await c.QueryAsync("dbo.sp_GetOpenIncidents", new { TenantId = Tenant(u) }, commandType: System.Data.CommandType.StoredProcedure));
}).RequireAuthorization("Operator");

v1.MapPost("/incidents/{id:long}/resolve", async (long id, ResolveDto dto, ClaimsPrincipal u) =>
{
    await using var c = new SqlConnection(Conn());
    await c.ExecuteAsync("dbo.sp_ResolveIncident", new { TenantId = Tenant(u), Id = id, Remediation = dto.Remediation }, commandType: System.Data.CommandType.StoredProcedure);
    return Results.NoContent();
}).RequireAuthorization("Operator");

app.Run();
record ResolveDto(string Remediation);
public partial class Program { }
