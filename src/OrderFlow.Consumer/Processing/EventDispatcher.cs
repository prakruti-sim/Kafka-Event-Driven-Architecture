using System.Text.Json;
using OrderFlow.Consumer.Handlers;
using OrderFlow.Consumer.Resilience;
using OrderFlow.Contracts;
using OrderFlow.Shared;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace OrderFlow.Consumer.Processing;

/// <summary>
/// Maps (topic, event-type) to a handler, deserializes the payload, and invokes it.
///
/// A fresh DI scope is created per dispatch, which is also per retry attempt. That is
/// what lets handlers take scoped dependencies (a DbContext, a unit of work) safely even
/// though the consumer itself is a singleton background service.
/// </summary>
public sealed class EventDispatcher(
    IServiceScopeFactory scopeFactory,
    ILogger<EventDispatcher> logger)
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public async Task DispatchAsync(string topic, string? eventType, string payload, CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var services = scope.ServiceProvider;

        switch (topic, eventType)
        {
            case (KafkaTopics.Orders, OrderCreatedEvent.Type):
                await InvokeAsync<OrderCreatedEvent>(services, payload, cancellationToken);
                break;

            case (KafkaTopics.Shipments, OrderShippedEvent.Type):
                await InvokeAsync<OrderShippedEvent>(services, payload, cancellationToken);
                break;

            case (KafkaTopics.Deliveries, OrderDeliveredEvent.Type):
                await InvokeAsync<OrderDeliveredEvent>(services, payload, cancellationToken);
                break;

            case (KafkaTopics.Notifications, NotificationRequestedEvent.Type):
                await InvokeAsync<NotificationRequestedEvent>(services, payload, cancellationToken);
                break;

            default:
                // No handler is registered for this combination, and no amount of retrying
                // will create one, so this is permanent by definition.
                logger.LogError("UNROUTABLE | topic={Topic} eventType={EventType}", topic, eventType ?? "(missing)");
                throw new PermanentEventException(
                    $"No handler registered for topic '{topic}' with event type '{eventType ?? "(missing)"}'.");
        }
    }

    private static async Task InvokeAsync<TEvent>(
        IServiceProvider services, string payload, CancellationToken cancellationToken)
        where TEvent : class, IEvent
    {
        TEvent @event;

        try
        {
            @event = JsonSerializer.Deserialize<TEvent>(payload, JsonOptions)
                     ?? throw new PermanentEventException($"Payload deserialized to null for {typeof(TEvent).Name}.");
        }
        catch (JsonException ex)
        {
            // Malformed payload: a classic poison message. Retrying re-parses the same
            // bytes and fails identically, so fail fast instead of burning the retry budget.
            throw new PermanentEventException($"Malformed JSON payload for {typeof(TEvent).Name}: {ex.Message}", ex);
        }

        var handler = services.GetRequiredService<IEventHandler<TEvent>>();
        await handler.HandleAsync(@event, cancellationToken);
    }
}
