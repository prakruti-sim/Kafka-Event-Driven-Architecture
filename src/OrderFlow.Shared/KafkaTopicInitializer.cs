using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace OrderFlow.Shared;

/// <summary>
/// Runs before any other hosted service: waits for the broker, then provisions topics.
/// Registered first so the consumer never subscribes to a topic that does not exist.
/// </summary>
public sealed class KafkaTopicInitializer(
    KafkaAdminService admin,
    ILogger<KafkaTopicInitializer> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Waiting for Kafka broker...");
        await admin.WaitForBrokerAsync(ct: cancellationToken);
        await admin.EnsureTopicsExistAsync(KafkaTopics.All, cancellationToken);
        logger.LogInformation("Kafka topic provisioning complete");
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
