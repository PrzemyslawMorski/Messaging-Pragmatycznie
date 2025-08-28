using Microsoft.Extensions.Logging;
using Neuroglia.AsyncApi.v3;
using TicketFlow.CourseUtils;
using TicketFlow.Services.Tickets.Core.Data.Models;
using TicketFlow.Services.Tickets.Core.Data.Repositories;
using TicketFlow.Services.Tickets.Core.Initializers;
using TicketFlow.Services.Tickets.Core.Messaging.Publishing;
using TicketFlow.Shared.AsyncAPI;
using TicketFlow.Shared.Commands;
using TicketFlow.Shared.Exceptions;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Tickets.Core.Commands.AssignAgentToTicket;

internal sealed class AssignAgentToTicketHandler(ITicketsRepository repository, IMessagePublisher publisher,
    ILogger<AssignAgentToTicketHandler> logger) : ICommandHandler<AssignAgentToTicket>
{
    public async Task HandleAsync(AssignAgentToTicket command, CancellationToken cancellationToken = default)
    {
        var ticket = await repository.GetAsync(command.TicketId, cancellationToken);

        if (ticket is null)
        {
            throw new TicketFlowException($"Ticket with id {command.TicketId} was not found.");
        }
        
        ticket.AssignTo(command.AgentId);
        await repository.UpdateAsync(ticket, cancellationToken);

        await PublishAgentAssigned(command, cancellationToken, ticket);
        logger.LogInformation($"Assigned agent to ticket with id {ticket.Id} to agent with id {command.AgentId}");
    }

    [Operation(Conventions.Operation.PublishPrefix + "AgentAssignedToTicket", V3OperationAction.Send, Conventions.Ref.ChannelPrefix + "AgentAssignedToTicket", Description = "Notify that agent was assigned to ticket")]
    private async Task PublishAgentAssigned(AssignAgentToTicket command, CancellationToken cancellationToken, Ticket ticket)
    {
        var ticketStatusChanged = new AgentAssignedToTicket(command.TicketId, ticket.Version);
        await publisher.PublishAsync(
            message: ticketStatusChanged, 
            routingKey: "agent-assigned", 
            cancellationToken: cancellationToken);
    }
}