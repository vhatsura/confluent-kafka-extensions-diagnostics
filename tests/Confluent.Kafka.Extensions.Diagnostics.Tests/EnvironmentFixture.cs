using TestEnvironment.Docker;
using TestEnvironment.Docker.Containers.Kafka;

namespace Confluent.Kafka.Extensions.Diagnostics.Tests;

public class EnvironmentFixture : IAsyncLifetime, IAsyncDisposable
{
    private const string KafkaContainerName = "kafka-diagnostics";

    private readonly IDockerEnvironment _dockerEnvironment;


    public EnvironmentFixture()
    {
        _dockerEnvironment = new DockerEnvironmentBuilder()
            .AddKafkaContainer(p => p with
            {
                Name = KafkaContainerName, ImageName = "dougdonohoe/kafka-zookeeper", Tag = "2.6.0"
            })
            .Build();
    }

    public string KafkaBootstrapServers
    {
        get
        {
            var kafkaContainer = _dockerEnvironment.GetContainer<KafkaContainer>(KafkaContainerName);

            if (kafkaContainer == null)
                throw new InvalidOperationException("Kafka container not found");

            return kafkaContainer.GetUrl();
        }
    }

    public async Task InitializeAsync()
    {
        await _dockerEnvironment.UpAsync();
    }

    async Task IAsyncLifetime.DisposeAsync()
    {
        await DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _dockerEnvironment.DownAsync();
    }
}
