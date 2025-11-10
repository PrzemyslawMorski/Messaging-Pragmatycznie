using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Inquiries.Core.Messaging.Consuming.AgentAssignedToTicket;

public record AgentAssignedToTicket(Guid TicketId, int Version) : IMessage;

