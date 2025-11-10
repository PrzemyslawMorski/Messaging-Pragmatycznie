using TicketFlow.Services.Inquiries.Core.Data.Repositories;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Inquiries.Core.Messaging.Consuming.AgentAssignedToTicket;

public class AgentAssignedToTicketHandler(IInquiriesRepository inquiriesRepository) : IMessageHandler<AgentAssignedToTicket>
{
    public async Task HandleAsync(AgentAssignedToTicket message, CancellationToken cancellationToken = default)
    {
        var inquiry = await inquiriesRepository.GetByTicketIdAsync(message.TicketId, cancellationToken);

        if (inquiry == null)
        {
            // Inquiry might not exist or might not be linked to a ticket yet
            // This is not necessarily an error - just return
            return;
        }
        
        inquiry.SetInProgress();
        await inquiriesRepository.UpdateAsync(inquiry, cancellationToken);
    }
}

