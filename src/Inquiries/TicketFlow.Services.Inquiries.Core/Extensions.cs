using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using TicketFlow.Services.Inquiries.Core.Data;
using TicketFlow.Services.Inquiries.Core.Data.Repositories;
using TicketFlow.Services.Inquiries.Core.LanguageDetection;
using TicketFlow.Services.Inquiries.Core.Messaging;
using TicketFlow.Services.SystemMetrics.Generator;
using TicketFlow.Shared.AnomalyGeneration;
using TicketFlow.Shared.App;
using TicketFlow.Shared.Commands;
using TicketFlow.Shared.Data;
using TicketFlow.Shared.Messaging;
using TicketFlow.Shared.Messaging.Deduplication;
using TicketFlow.Shared.Messaging.Outbox;
using TicketFlow.Shared.Messaging.RabbitMQ;
using TicketFlow.Shared.Messaging.Resiliency;
using TicketFlow.Shared.Messaging.Topology;
using TicketFlow.Shared.Observability;
using TicketFlow.Shared.Queries;
using TicketFlow.Shared.Serialization;

namespace TicketFlow.Services.Inquiries.Core;

public static class Extensions
{
    public static IServiceCollection AddCore(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddScoped<IInquiriesRepository, InquiriesRepository>();
        services.AddScoped<Messaging.Consuming.Demultiplexing.TicketChangesHandler>();
        
        services
            .AddHttpClient()
            .AddSerialization()
            .AddApp(configuration)
            .AddCommands()
            .AddQueries()
            .AddLogging()
            .AddMessaging(configuration, x => x
                .UseRabbitMq()
                .UseMessageConsumerConvention<DontUseConventionalTopology>()
                .UseAnomalies()
                .UseResiliency())
            .AddPostgres<InquiriesDbContext>(configuration)
            .AddLanguageDetection(configuration)
            .AddSystemMetrics(configuration)
            .AddObservability(configuration);

        services.AddHostedService<InquiriesConsumerService>();
        services.AddHostedService<InquiriesTopologyInitializer>();
        
        return services;
    }
}