using Neuroglia.AsyncApi.v3;
using TicketFlow.CourseUtils;
using TicketFlow.Services.Tickets.Core.Data.Models;
using TicketFlow.Services.Tickets.Core.Data.Repositories;
using TicketFlow.Shared.AsyncAPI;
using TicketFlow.Shared.Messaging;

namespace TicketFlow.Services.Tickets.Core.Messaging.Consuming.InquirySubmitted;

public sealed class InquirySubmittedHandler(ITicketsRepository repository, IMessagePublisher messagePublisher) : IMessageHandler<InquirySubmitted>
{
    public async Task HandleAsync(InquirySubmitted message, CancellationToken cancellationToken = default)
    {
        if (!FeatureFlags.UseListenToYourselfExample)
        {
            await HandleDefault(message, cancellationToken);
        }
        else
        {
            await HandleWithListenToYourself(message, cancellationToken);
        }
    }

    private async Task HandleDefault(InquirySubmitted message, CancellationToken cancellationToken)
    {
        var (id, name, email, title, description, category, languageCode, _) = message;

        if (await repository.ExistsAsync(id, cancellationToken))
        {
            return;
        }

        var categoryValid = Enum.TryParse<TicketCategory>(category, out var categoryParsed);
        if (categoryValid is false)
        {
            categoryParsed = TicketCategory.Other;
        }
        
        var ticket = new Ticket(id, name, email, title, description, categoryParsed, languageCode);
        
        var scheduledAction = await repository.GetScheduledAction(message.Id, cancellationToken);

        if (scheduledAction is not null)
        {
            ticket.SetTranslation(scheduledAction.TranslatedText);
        }
        else if (ticket.IsEnglish is false)
        {
            ticket.WaitForScheduledActions();
        }
        
        await repository.AddAsync(ticket, cancellationToken);

        var ticketCreatedMessage = new Publishing.TicketCreated(
            Id: ticket.Id, 
            InquiryId: message.Id, 
            Name: ticket.Name,
            Email: ticket.Email,
            Title: ticket.Title,
            Description: ticket.Description,
            Category: ticket.Category.ToString(),
            LanguageCode: ticket.LanguageCode);
        
        await PublishTicketCreated(cancellationToken, ticketCreatedMessage);
    }

    private async Task HandleWithListenToYourself(InquirySubmitted message, CancellationToken cancellationToken)
    {
        var (id, name, email, title, description, category, languageCode, _) = message;

        if (await repository.ExistsAsync(id, cancellationToken))
        {
            return;
        }

        var categoryValid = Enum.TryParse<TicketCategory>(category, out var categoryParsed);
        if (categoryValid is false)
        {
            categoryParsed = TicketCategory.Other;
        }
        
        var ticket = new Ticket(Guid.NewGuid(), name, email, title, description, categoryParsed, languageCode);
        
        var scheduledAction = await repository.GetScheduledAction(message.Id, cancellationToken);

        if (scheduledAction is not null)
        {
            ticket.SetTranslation(scheduledAction.TranslatedText);
        }
        else if (ticket.IsEnglish is false)
        {
            ticket.WaitForScheduledActions();
        }

        var ticketCreatedMessage = new Publishing.TicketCreated(
            Id: ticket.Id, 
            InquiryId: message.Id, 
            Name: ticket.Name,
            Email: ticket.Email,
            Title: ticket.Title,
            Description: ticket.Description,
            Category: ticket.Category.ToString(),
            LanguageCode: ticket.LanguageCode);
        
        await PublishTicketCreated(cancellationToken, ticketCreatedMessage);
    }
    
    [Operation(Conventions.Operation.PublishPrefix + "TicketCreated", V3OperationAction.Send, Conventions.Ref.ChannelPrefix + "TicketCreated", Description = "Notify that ticket was created")]
    private async Task PublishTicketCreated(CancellationToken cancellationToken, Publishing.TicketCreated ticketCreatedMessage)
    {
        await messagePublisher.PublishAsync(ticketCreatedMessage, cancellationToken: cancellationToken);
    }
}