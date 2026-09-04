using System.ComponentModel.DataAnnotations;
using OrderFlow.Contracts;

namespace OrderFlow.Producer.Models;

/// <summary>Request body for POST /api/orders.</summary>
public sealed class CreateOrderRequest
{
    /// <summary>
    /// Customer id. Use the literal <c>CUST-FAIL</c> to make the consumer's handler
    /// throw, which exercises the retry-then-skip path.
    /// </summary>
    [Required, StringLength(64, MinimumLength = 1)]
    public string CustomerId { get; set; } = string.Empty;

    [Required, StringLength(200, MinimumLength = 1)]
    public string CustomerName { get; set; } = string.Empty;

    [StringLength(3, MinimumLength = 3)]
    public string Currency { get; set; } = "USD";

    [Required, MinLength(1)]
    public List<OrderItemRequest> Items { get; set; } = [];

    public (bool IsValid, string? Error) Validate()
    {
        if (Items.Count == 0)
            return (false, "At least one order item is required.");
        if (Items.Any(i => i.Quantity <= 0))
            return (false, "Every item quantity must be greater than zero.");
        if (Items.Any(i => i.UnitPrice < 0))
            return (false, "Item unit price cannot be negative.");
        if (Items.Any(i => string.IsNullOrWhiteSpace(i.ProductId)))
            return (false, "Every item requires a productId.");
        return (true, null);
    }

    public OrderCreatedEvent ToEvent(Guid orderId, string correlationId) => new()
    {
        OrderId = orderId,
        CorrelationId = correlationId,
        CustomerId = CustomerId,
        CustomerName = CustomerName,
        Currency = Currency,
        TotalAmount = Items.Sum(i => i.UnitPrice * i.Quantity),
        Items = [.. Items.Select(i => new OrderItem
        {
            ProductId = i.ProductId,
            ProductName = i.ProductName,
            Quantity = i.Quantity,
            UnitPrice = i.UnitPrice
        })]
    };
}

public sealed class OrderItemRequest
{
    [Required] public string ProductId { get; set; } = string.Empty;
    public string ProductName { get; set; } = string.Empty;
    [Range(1, 10_000)] public int Quantity { get; set; } = 1;
    [Range(0, 1_000_000)] public decimal UnitPrice { get; set; }
}
