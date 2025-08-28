using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Neuroglia.AsyncApi;
using Neuroglia.AsyncApi.Generation;
using TicketFlow.Services.SLA.Core;
using TicketFlow.Services.SLA.Core.Data.Models;
using TicketFlow.Services.SLA.Core.Data.Repositories;
using TicketFlow.Shared.AnomalyGeneration.HttpApi;
using TicketFlow.Shared.AspNetCore;
using TicketFlow.Shared.AsyncAPI;

var builder = WebApplication.CreateBuilder(args);
builder.Services
    .AddCore(builder.Configuration)
    .AddApiForFrontendConfigured()
    .AddDocumentation();

var app = builder.Build();

app.MapGet("/sla/{serviceType}/{serviceSourceId}/deadline-reminders", async (
    [FromRoute] string serviceType,
    [FromRoute] string serviceSourceId,
    [FromServices] ISLARepository repository,
    CancellationToken cancellationToken) =>
{
    var parseSuccess = Enum.TryParse<ServiceType>(serviceType, out var serviceTypeParsed);
    if (parseSuccess is false)
    {
        return Results.BadRequest("Invalid service type");
    }
    
    var result = await repository.GetRemindersFor(serviceTypeParsed, serviceSourceId, cancellationToken);

    if (result is null)
    {
        return Results.NotFound("Service not found");
    }
    
    return Results.Ok(result);
});

app.ExposeApiForFrontend();
app.UseAnomalyEndpoints();

app.UseDocumentation();

app.Run();