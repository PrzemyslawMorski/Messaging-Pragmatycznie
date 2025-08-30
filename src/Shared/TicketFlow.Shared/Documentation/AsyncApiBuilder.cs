using System.Collections;
using System.Reflection;
using Json.Schema.Generation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Neuroglia;
using Neuroglia.AsyncApi;
using Neuroglia.AsyncApi.Bindings;
using Neuroglia.AsyncApi.FluentBuilders;
using Neuroglia.AsyncApi.FluentBuilders.v3;
using Neuroglia.AsyncApi.Generation;
using Neuroglia.AsyncApi.v3;
using Newtonsoft.Json.Schema;
using NJsonSchema;
using TicketFlow.Shared.App;
using TicketFlow.Shared.Messaging;
using TicketFlow.Shared.Messaging.Topology;

namespace TicketFlow.Shared.AsyncAPI;

public class AsyncApiBuilder(IServiceProvider serviceProvider) : BackgroundService, IAsyncApiDocumentProvider
{
    private static readonly List<IAsyncApiDocument> _documents = new();
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topologyDescription = serviceProvider.GetService<TopologyDescription>();
        var topologyReadiness = serviceProvider.GetService<TopologyReadinessAccessor>();
        var logger = serviceProvider.GetRequiredService<ILogger<AsyncApiBuilder>>();
        
        while (stoppingToken.IsCancellationRequested is false)
        {
            await Task.Delay(2_000, stoppingToken);
            try
            {
                if (topologyReadiness!.TopologyProvisioned && topologyDescription!.TopologyDescribed)
                {
                    logger.LogInformation("Topology is ready and described!");
                    break;
                }
                logger.LogInformation("Topology not ready to be described; waiting...");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, ex.Message);
            }
        }
        
        await ProduceAsyncApiDocument();
        logger.LogInformation("Topology described in AsyncAPI format!");
    }
    
    private Task ProduceAsyncApiDocument()
    {
        var topologyDescription = serviceProvider.GetService<TopologyDescription>();
        var builder = serviceProvider.GetRequiredService<IAsyncApiDocumentBuilder>();
        var appSettings = serviceProvider.GetRequiredService<IOptions<AppOptions>>();
        var v3 = builder
            .UsingAsyncApiV3()
            .WithTitle($"TicketFlow - {appSettings.Value.AppName}")
            .WithVersion("1.0.0");

        var messageConfigurations = RegisterMessages(v3);
        RegisterConsumerSide(topologyDescription!, v3, messageConfigurations);
        RegisterPublisherSide(v3, messageConfigurations);

        var document = builder.Build();
        _documents.Add(document);

        return Task.CompletedTask;
    }

    private static void RegisterConsumerSide(TopologyDescription topologyDescription, IV3AsyncApiDocumentBuilder v3,
        Dictionary<string, Action<IV3MessageDefinitionBuilder>> messageConfigurations)
    {
        
        foreach (var channel in topologyDescription!.Channels)
        {
            v3.WithChannel(Conventions.Channel.Name(channel.Key), setup =>
            {
                foreach (var binding in (channel.Value.Bindings ?? new ChannelBindingDefinitionCollection()).AsEnumerable())
                {
                    setup.WithBinding(binding);
                }

                foreach (var message in channel.Value.Messages)
                {
                    setup.WithMessage(message.Key, msgSetup =>
                    {
                        var hasMatchingConfig = messageConfigurations.TryGetValue(message.Key, out var config);
                        if (hasMatchingConfig)
                        {
                            config!(msgSetup);
                        }
                        
                        foreach (var binding in (message.Value.Bindings ?? new MessageBindingDefinitionCollection())
                                 .AsEnumerable())
                        {
                            msgSetup.WithBinding(binding);
                        }
                    });
                }
            });
        }

        foreach (var operation in topologyDescription!.Operations)
        {
            v3.WithOperation(operation.Key, setup =>
            {
                setup.WithAction(operation.Value.Action);
                setup.WithChannel(operation.Value.Channel.Reference);
                foreach (var msg in operation.Value.Messages)
                {
                    setup.WithMessage(msg.Reference);
                }

                foreach (var binding in (operation.Value.Bindings ?? new OperationBindingDefinitionCollection()).AsEnumerable())
                {
                    setup.WithBinding(binding);
                }
            });
        }
    }
    
    private static void RegisterPublisherSide(IV3AsyncApiDocumentBuilder v3, Dictionary<string, Action<IV3MessageDefinitionBuilder>> messageConfigurations)
    {
        var callingAssembly = Assembly.GetEntryAssembly();
        var ownedReferencedAssemblies = callingAssembly.GetReferencedAssemblies()
            .Where(x => x.Name.StartsWith("TicketFlow"))
            .Select(Assembly.Load)
            .ToList();
        
        var types = new[] { callingAssembly }.Concat(ownedReferencedAssemblies)
            .Select(x => x.GetTypes())
            .SelectMany(x => x);
        
        foreach (var type in types)
        {
            var methods = type.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static);
            
            foreach (var method in methods)
            {
                var operationAttribute = method.GetCustomAttribute<OperationAttribute>();
                if (operationAttribute != null)
                {
                    var channelName = Conventions.Channel.DereferenceNameFromChannelRef(operationAttribute.Channel);
                    var messageName = Conventions.Message.FromChannelName(channelName);
                    
                    v3.WithChannel(channelName, setup =>
                    {
                        setup.WithMessage(messageName, msgSetup =>
                        {
                            var hasMatchingConfig = messageConfigurations.TryGetValue(messageName, out var config);
                            if (hasMatchingConfig)
                            {
                                config!(msgSetup);
                            }
                        });
                    });
                    
                    v3.WithOperation(operationAttribute.Name, setup =>
                    {
                        setup.WithAction(operationAttribute.Action)
                            .WithChannel(operationAttribute.Channel)
                            .WithDescription(operationAttribute.Description)
                            .WithMessage(Conventions.Ref.ChannelMessage(channelName, messageName));
                    });
                }
            }
        }
    }

    private static Dictionary<string, Action<IV3MessageDefinitionBuilder>> RegisterMessages(IV3AsyncApiDocumentBuilder v3)
    {
        var callingAssembly = Assembly.GetEntryAssembly();
        var ownedReferencedAssemblies = callingAssembly.GetReferencedAssemblies()
            .Where(x => x.Name.StartsWith("TicketFlow"))
            .Select(Assembly.Load)
            .ToList();
        
        var types = new[] { callingAssembly }.Concat(ownedReferencedAssemblies)
            .Select(x => x.GetTypes())
            .SelectMany(x => x);

        var messageConfigurations = new Dictionary<string, Action<IV3MessageDefinitionBuilder>>();
        
        foreach (var type in types
                    .Where(t => typeof(IMessage).IsAssignableFrom(t))
                    .Where(t => t.IsAbstract is false && t.IsInterface is false)
                    .Where(t => t.ContainsGenericParameters is false))
        {
            v3.WithMessageComponent(type.Name, message =>
            {
                var messageConfig = (IV3MessageDefinitionBuilder msg) =>
                {
                    msg.WithPayloadSchema(schema => schema
                        .WithJsonSchema(jsonSchema => jsonSchema
                        .FromType(type, JsonSchemaGeneratorConfiguration.Default)));
                    msg.WithName(type.Name);
                    msg.WithTitle(type.Name);
                };

                messageConfig(message);
                messageConfigurations.Add(type.Name, messageConfig);
            });
        }

        return messageConfigurations;
    }

    public IEnumerator<IAsyncApiDocument> GetEnumerator()
    {
        return _documents.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public virtual Task<IAsyncApiDocument?> GetDocumentAsync(string title, string version, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(title);
        ArgumentException.ThrowIfNullOrWhiteSpace(version);
        return Task.FromResult(_documents!.FirstOrDefault(d => (d.Title.Equals(title, StringComparison.OrdinalIgnoreCase) 
                                                                || d.Title.ToKebabCase().Equals(title, StringComparison.OrdinalIgnoreCase)) && d.Version.Equals(version, StringComparison.OrdinalIgnoreCase)));
    }

    /// <inheritdoc/>
    public virtual Task<IAsyncApiDocument?> GetDocumentAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        return Task.FromResult(_documents!.FirstOrDefault(d => !string.IsNullOrWhiteSpace(d.Id) && d.Id.Equals(id, StringComparison.OrdinalIgnoreCase)));
    }
}