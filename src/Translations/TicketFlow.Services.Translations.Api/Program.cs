using Microsoft.AspNetCore.Mvc;
using TicketFlow.Services.Translations.Core;
using TicketFlow.Services.Translations.Core.SynchronousIntegration;
using TicketFlow.Shared.AsyncAPI;
using TicketFlow.Shared.Queries;

var builder = WebApplication.CreateBuilder(args);
builder.Services
    .AddCore(builder.Configuration)
    .AddDocumentation();

var app = builder.Build();

app.MapGet("/", () => "Translations Service");

app.MapPost("/translations", async (
    [FromServices] IQueryHandler<GetTranslatedTextSynchronously, string> handler,
    [FromBody] GetTranslatedTextSynchronously query,
    CancellationToken cancellationToken) =>
{
    var translatedText = await handler.HandleAsync(query, cancellationToken);
    return Results.Ok(translatedText);
});

app.UseDocumentation();
app.Run();

public partial class Program { }
