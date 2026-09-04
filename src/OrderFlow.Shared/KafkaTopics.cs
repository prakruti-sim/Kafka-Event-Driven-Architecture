namespace OrderFlow.Shared;

public static class KafkaTopics
{
    public const string Orders        = "orders";
    public const string Shipments     = "shipments";
    public const string Deliveries    = "deliveries";
    public const string Notifications = "notifications";

    /// <summary>Business topics the consumer subscribes to.</summary>
    public static readonly string[] Business = [Orders, Shipments, Deliveries, Notifications];

    /// <summary>Every topic the admin client provisions at startup.</summary>
    public static readonly string[] All = Business;
}
