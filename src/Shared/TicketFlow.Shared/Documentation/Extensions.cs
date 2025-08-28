using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Neuroglia.AsyncApi;
using Neuroglia.AsyncApi.Generation;
using Neuroglia.AsyncApi.IO;

namespace TicketFlow.Shared.AsyncAPI;

public static class Extensions
{
    public static IServiceCollection AddDocumentation(this IServiceCollection services)
    {
        services
            .AddEndpointsApiExplorer()
            .AddSwaggerGen()
            .AddAsyncApi()
            .AddAsyncApiUI()
            .AddRazorPages();
        services
            .AddHostedService<AsyncApiBuilder>()
            .TryAddSingleton<IAsyncApiDocumentProvider, AsyncApiBuilder>();
        services.TryAddTransient<IJsonSchemaExampleGenerator, JsonSchemaExampleGenerator>();
        services.TryAddTransient<IAsyncApiDocumentGenerator, AsyncApiDocumentGenerator>();

        return services;
    }

    public static void UseDocumentation(this WebApplication app)
    {
        app.UseStaticFiles();
        app.UseRouting();
        app.UseAuthorization();
        app.MapAsyncApiDocuments();
        app.MapRazorPages();
        app.ExposeAsyncApiYaml();
    }

    public static void ExposeAsyncApiYaml(this IEndpointRouteBuilder routeBuilder)
    {
        routeBuilder.MapGet("asyncapi/asyncapi.yaml", async (
            [FromServices] IAsyncApiDocumentWriter writer,
            [FromServices] IAsyncApiDocumentProvider documentProvider) =>
        {
            using MemoryStream stream = new();
            foreach (var document in documentProvider)
            {
                await writer.WriteAsync(document, stream, AsyncApiDocumentFormat.Yaml, CancellationToken.None);
            }

            return Results.Bytes(stream.ToArray(), "application/text", "asyncapi.yaml");
        });
    }
}