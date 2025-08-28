using Microsoft.AspNetCore.Mvc;
using Neuroglia.AsyncApi;
using TicketFlow.CourseUtils;
using TicketFlow.Services.Inquiries.Core;
using TicketFlow.Services.Inquiries.Core.Commands.SubmitInquiry;
using TicketFlow.Services.Inquiries.Core.Commands.SubmitInquirySynchronously;
using TicketFlow.Services.Inquiries.Core.Queries;
using TicketFlow.Shared.AnomalyGeneration.HttpApi;
using TicketFlow.Shared.AspNetCore;
using TicketFlow.Shared.AsyncAPI;
using TicketFlow.Shared.Commands;
using TicketFlow.Shared.Queries;

var builder = WebApplication.CreateBuilder(args);
builder.Services
    .AddCore(builder.Configuration)
    .AddApiForFrontendConfigured()
    .AddDocumentation();

var app = builder.Build();

app.ExposeApiForFrontend();
app.UseAnomalyEndpoints();

app.MapGet("/", () => "Inquiries Service");

app.MapGet("/inquiries", async (
    [FromQuery] int page,
    [FromQuery] int limit,
    [FromServices] IQueryHandler<ListInquiries, InquiriesListDto> handler,
    CancellationToken cancellationToken) 
    => Results.Ok((object?)await handler.HandleAsync(new(page, limit), cancellationToken)));

app.MapPost("/inquiries/submit", async (
    [FromBody] SubmitInquiry command, 
    [FromServices] ICommandHandler<SubmitInquirySynchronously> synchronousHandler,
    [FromServices] ICommandHandler<SubmitInquiry> handler,
    CancellationToken cancellationToken) =>
{
    if (FeatureFlags.UseSynchronousIntegration)
    {
        var synchronousCommand = new SubmitInquirySynchronously(command.Name, command.Email, command.Title, command.Description, command.Category);
        await synchronousHandler.HandleAsync(synchronousCommand);
        return Results.Ok();
    }
    else
    {
        await handler.HandleAsync(command, cancellationToken);
        return Results.Ok();
    }
});

app.MapPost("/inquiries/submit-sync", async ([FromBody] SubmitInquirySynchronously command, [FromServices] ICommandHandler<SubmitInquirySynchronously> handler,
    CancellationToken cancellationToken) =>
{
    await handler.HandleAsync(command, cancellationToken);
    return Results.Ok();
});

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(options => // UseSwaggerUI is called only in Development.
    {
        options.SwaggerEndpoint("/swagger/v1/swagger.json", "v1");
        options.RoutePrefix = string.Empty;
    });
}

app.UseDocumentation();
app.Run();

public partial class Program { }