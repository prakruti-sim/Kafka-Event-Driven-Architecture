using OrderFlow.Contracts;
using OrderFlow.Producer.Models;
using OrderFlow.Shared;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss.fff ";
});
builder.Logging.AddFilter("Confluent", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);

builder.Services.AddOrderFlowInfrastructure(builder.Configuration);
builder.Services.AddProblemDetails();

var app = builder.Build();

// ── Health: used as the docker-compose healthcheck ───────────────────────────
app.MapGet("/health", () => Results.Ok(new { status = "healthy", service = "producer-api" }));

// ── Root: lists the available endpoints, since there is no Swagger UI ────────
app.MapGet("/", () => Results.Ok(new
{
    service = "OrderFlow.Producer",
    endpoints = new[]
    {
        "GET  /health",
        "POST /api/orders               - publish one OrderCreatedEvent",
        "POST /api/orders/bulk?count=N  - publish N synthetic orders",
        "GET  /api/kafka/offsets        - log the consumer group's committed offsets"
    },
    failureDemo = new
    {
        alwaysFails = "set customerId to CUST-FAIL  -> retries exhausted, failure logged, message skipped",
        transient   = "set customerId to CUST-FLAKY -> fails twice, succeeds on the third attempt"
    }
}));

// ── Publish a single order event ─────────────────────────────────────────────
// Returns 202 Accepted with the broker's partition and offset, which is what makes
// the write independently verifiable in Kafka UI.
app.MapPost("/api/orders", async (
    CreateOrderRequest request,
    IEventProducer producer,
    ILogger<Program> logger,
    CancellationToken ct) =>
{
    var (isValid, error) = request.Validate();
    if (!isValid)
        return Results.Problem(title: "Invalid order", detail: error, statusCode: StatusCodes.Status400BadRequest);

    var orderId = Guid.NewGuid();
    var correlationId = $"ORD-{orderId.ToString()[..8].ToUpperInvariant()}";
    var @event = request.ToEvent(orderId, correlationId);

    logger.LogInformation(
        "API REQUEST | POST /api/orders | correlationId={CorrelationId} customer={CustomerId} items={ItemCount} total={Currency} {Amount:F2}",
        correlationId, request.CustomerId, request.Items.Count, @event.Currency, @event.TotalAmount);

    try
    {
        // Key by order id so every event for this order lands on one partition,
        // which is what preserves per-order ordering.
        var result = await producer.PublishAsync(KafkaTopics.Orders, @event, orderId.ToString(), ct);

        return Results.Accepted($"/api/orders/{orderId}", new PublishResponse(
            orderId, @event.EventId, correlationId, @event.EventType,
            result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Key));
    }
    catch (Exception ex)
    {
        // The publish is synchronous from the caller's perspective: if the broker did not
        // ack the write, the client must know the event does not exist.
        logger.LogError(ex, "API FAILED | publish rejected | correlationId={CorrelationId}", correlationId);
        return Results.Problem(
            title: "Event publish failed",
            detail: $"Kafka rejected the write: {ex.Message}",
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
});

// ── Publish a batch, for lag and partition-spread demos ──────────────────────
app.MapPost("/api/orders/bulk", async (
    [FromQuery] int count,
    IEventProducer producer,
    ILogger<Program> logger,
    CancellationToken ct) =>
{
    if (count is < 1 or > 500)
        return Results.Problem(title: "Invalid count", detail: "count must be between 1 and 500.",
            statusCode: StatusCodes.Status400BadRequest);

    logger.LogInformation("API REQUEST | POST /api/orders/bulk | count={Count}", count);

    var published = new List<PublishResponse>(count);

    for (var i = 1; i <= count; i++)
    {
        var orderId = Guid.NewGuid();
        var correlationId = $"BULK-{orderId.ToString()[..8].ToUpperInvariant()}";

        var @event = new OrderCreatedEvent
        {
            OrderId = orderId,
            CorrelationId = correlationId,
            CustomerId = $"CUST-{i:D3}",
            CustomerName = $"Load Test Customer {i}",
            Currency = "USD",
            TotalAmount = 19.99m * i,
            Items =
            [
                new OrderItem
                {
                    ProductId = $"PROD-{i:D3}",
                    ProductName = $"Demo Product {i}",
                    Quantity = 1,
                    UnitPrice = 19.99m * i
                }
            ]
        };

        var result = await producer.PublishAsync(KafkaTopics.Orders, @event, orderId.ToString(), ct);

        published.Add(new PublishResponse(orderId, @event.EventId, correlationId, @event.EventType,
            result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Key));
    }

    // Partition spread is the interesting part: distinct keys hash across all partitions.
    var partitionSpread = published
        .GroupBy(p => p.Partition)
        .OrderBy(g => g.Key)
        .ToDictionary(g => $"partition-{g.Key}", g => g.Count());

    return Results.Accepted(value: new
    {
        publishedCount = published.Count,
        partitionSpread,
        events = published
    });
});

// ── Inspect committed offsets for the consumer group ─────────────────────────
app.MapGet("/api/kafka/offsets", async (KafkaAdminService admin, CancellationToken ct) =>
{
    await admin.LogConsumerGroupLagAsync(KafkaTopics.Business, ct);
    return Results.Ok(new
    {
        message = "Committed offsets, high watermarks and lag written to the producer-api logs.",
        hint = "docker compose logs producer-api --tail 30"
    });
});

app.Run();
