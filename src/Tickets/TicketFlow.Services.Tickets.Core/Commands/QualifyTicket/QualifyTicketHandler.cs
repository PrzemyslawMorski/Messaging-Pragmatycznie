using Microsoft.Extensions.Logging;
using Neuroglia.AsyncApi.v3;
using TicketFlow.CourseUtils;
using TicketFlow.Services.Tickets.Core.Data.Models;
using TicketFlow.Services.Tickets.Core.Data.Repositories;
using TicketFlow.Services.Tickets.Core.Messaging.Publishing;
using TicketFlow.Shared.AsyncAPI;
using TicketFlow.Shared.Commands;
using TicketFlow.Shared.Exceptions;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Tickets.Core.Commands.QualifyTicket;

internal sealed class QualifyTicketHandler(ITicketsRepository repository, IMessagePublisher publisher,
    ILogger<QualifyTicketHandler> logger) : ICommandHandler<QualifyTicket>
{
    public async Task HandleAsync(QualifyTicket command, CancellationToken cancellationToken = default)
    {
        var ticket = await repository.GetAsync(command.TicketId, cancellationToken);

        if (ticket is null)
        {
            throw new TicketFlowException($"Ticket with id {command.TicketId} was not found.");
        }

        ticket.Qualify(command.TicketType, command.SeverityLevel);
        await repository.UpdateAsync(ticket, cancellationToken);
        
        await PublishTicketQualified(command, cancellationToken, ticket);

        if (ticket.Type is TicketType.Incident)
        {
            await PublishIncidentCreated(cancellationToken, ticket);
        }
        
        logger.LogInformation($"Ticket with id {ticket.Id} has been qualified as {command.TicketType}");
    }

    [Operation(Conventions.Operation.PublishPrefix + "PublishIncidentCreated", V3OperationAction.Send, Conventions.Ref.ChannelPrefix + "PublishIncidentCreated", Description = "Notify that incident was created")]
    private async Task PublishIncidentCreated(CancellationToken cancellationToken, Ticket ticket)
    {
        var incidentCreatedMessage = new IncidentCreated(ticket.Id, ticket.Version);
        await publisher.PublishAsync(incidentCreatedMessage, cancellationToken: cancellationToken);
    }

    [Operation(Conventions.Operation.PublishPrefix + "TicketQualified", V3OperationAction.Send, Conventions.Ref.ChannelPrefix + "TicketQualified", Description = "Notify that ticket was qualified")]
    private async Task PublishTicketQualified(QualifyTicket command, CancellationToken cancellationToken, Ticket ticket)
    {
        var ticketStatusChanged = new TicketQualified(command.TicketId, ticket.Version);
        await publisher.PublishAsync(
            message: ticketStatusChanged, 
            routingKey: "ticket-qualified", 
            cancellationToken: cancellationToken);
    }
}