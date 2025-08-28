using TicketFlow.Services.SystemMetrics.Core;
using TicketFlow.Shared.AnomalyGeneration.HttpApi;
using TicketFlow.Shared.AspNetCore;
using TicketFlow.Shared.AsyncAPI;

var builder = WebApplication.CreateBuilder(args);
builder.Services
    .AddCore(builder.Configuration)
    .AddApiForFrontendConfigured()
    .AddDocumentation();

var app = builder.Build();

app.ExposeApiForFrontend();
app.UseAnomalyEndpoints();
app.ExposeLiveMetrics();

app.UseDocumentation();

app.Run();