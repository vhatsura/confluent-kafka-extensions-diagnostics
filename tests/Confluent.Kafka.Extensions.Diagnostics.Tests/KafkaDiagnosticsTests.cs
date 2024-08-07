using System.Diagnostics;
using FluentAssertions;
using Snapshooter;
using Snapshooter.Xunit;
using Xunit.Extensions.AssemblyFixture;

namespace Confluent.Kafka.Extensions.Diagnostics.Tests;

public sealed class KafkaDiagnosticsTests : IAssemblyFixture<EnvironmentFixture>, IDisposable
{
    private readonly EnvironmentFixture _environmentFixture;
    private readonly IProducer<string, string> _producer;
    private readonly IProducer<string, string> _dependentProducer;
    private readonly IConsumer<string, string> _consumer;

    private readonly Func<MatchOptions, MatchOptions> _matchOptions;

    public KafkaDiagnosticsTests(EnvironmentFixture environmentFixture)
    {
        _environmentFixture = environmentFixture;

        var kafkaBootstrapServers = _environmentFixture.KafkaBootstrapServers;
        var kafkaClientConfig = new ClientConfig { BootstrapServers = kafkaBootstrapServers };

        _producer = new ProducerBuilder<string, string>(new ProducerConfig(kafkaClientConfig))
            .SetKeySerializer(Serializers.Utf8)
            .SetValueSerializer(Serializers.Utf8)
            .BuildWithInstrumentation();

        _dependentProducer = new DependentProducerBuilder<string, string>(_producer.Handle)
            .SetKeySerializer(Serializers.Utf8)
            .SetValueSerializer(Serializers.Utf8)
            .BuildWithInstrumentation();

        _consumer = new ConsumerBuilder<string, string>(
                new ConsumerConfig(new ClientConfig { BootstrapServers = kafkaBootstrapServers })
                {
                    GroupId = "group",
                    AutoOffsetReset = AutoOffsetReset.Earliest
                })
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(Deserializers.Utf8)
            .Build();

        _matchOptions = options => options.ExcludeField("Duration").ExcludeField("StartTimeUtc").ExcludeField("Id")
            .ExcludeField("RootId").ExcludeField("TagObjects");
    }

    [Fact]
    public async Task DependentProduceAsync()
    {
        // Arrange
        var snapshotName = Snapshot.FullName();
        using var listener = CreateActivityListener(activity =>
        {
            // Assert
            activity.Should().MatchSnapshot(snapshotName, _matchOptions);
        });
        ActivitySource.AddActivityListener(listener);

        // Act
        await _dependentProducer.ProduceAsync("produce_async_topic",
            new Message<string, string> { Key = "test", Value = "Hello World!" });
    }

    [Fact]
    public async Task DependentProduce()
    {
        // Arrange
        Activity? reportedActivity = null;
        using var listener = CreateActivityListener(activity =>
        {
            reportedActivity = activity;
        });
        ActivitySource.AddActivityListener(listener);

        var delivered = false;

        // Act
        _dependentProducer.Produce("produce_topic",
            new Message<string, string> { Key = "test", Value = "Hello World!" }, report =>
            {
                delivered = true;
            });

        int leftAttempts = 10;
        do
        {
            await Task.Delay(TimeSpan.FromMilliseconds(500));
        } while (!delivered && leftAttempts-- > 0);

        delivered.Should().BeTrue();
        reportedActivity.Should().NotBeNull();
        reportedActivity.Should().MatchSnapshot(_matchOptions);
    }


    [Fact]
    public async Task ProduceAsync()
    {
        // Arrange
        var snapshotName = Snapshot.FullName();
        using var listener = CreateActivityListener(activity =>
        {
            // Assert
            activity.Should().MatchSnapshot(snapshotName, _matchOptions);
        });
        ActivitySource.AddActivityListener(listener);

        // Act
        await _producer.ProduceAsync("produce_async_topic",
            new Message<string, string> { Key = "test", Value = "Hello World!" });
    }

    [Fact]
    public async Task Produce()
    {
        // Arrange
        Activity? reportedActivity = null;
        using var listener = CreateActivityListener(activity =>
        {
            reportedActivity = activity;
        });
        ActivitySource.AddActivityListener(listener);

        var delivered = false;

        // Act
        _producer.Produce("produce_topic",
            new Message<string, string> { Key = "test", Value = "Hello World!" }, report =>
            {
                delivered = true;
            });

        int leftAttempts = 10;
        do
        {
            await Task.Delay(TimeSpan.FromMilliseconds(500));
        } while (!delivered && leftAttempts-- > 0);

        delivered.Should().BeTrue();
        reportedActivity.Should().NotBeNull();
        reportedActivity.Should().MatchSnapshot(_matchOptions);
    }

    [Fact]
    public async Task Consume()
    {
        // Arrange
        await _producer.ProduceAsync("consume_topic",
            new Message<string, string> { Key = "test", Value = "Hello World!" });

        var snapshotName = Snapshot.FullName();
        using var listener = CreateActivityListener(activity =>
        {
            if (activity.OperationName.EndsWith("process"))
            {
                // Assert
                activity.Should().MatchSnapshot(snapshotName,
                    options => _matchOptions(options).IgnoreField("Tags[6].Value"));
            }
        });
        ActivitySource.AddActivityListener(listener);

        // Act
        _consumer.Subscribe("consume_topic");

        try
        {
            try
            {
                _consumer.ConsumeWithInstrumentation((result) =>
                {
                    Console.WriteLine(result.Message.Value);
                }, 2000);
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Error occured: {e.Error.Reason}");
            }
        }
        catch (OperationCanceledException)
        {
            _consumer.Close();
        }
    }

    private ActivityListener CreateActivityListener(Action<Activity> activityStopped)
    {
        return new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Confluent.Kafka.Extensions.Diagnostics",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.PropagationData,
            ActivityStopped = activityStopped
        };
    }

    public void Dispose()
    {
        _producer.Dispose();
        _consumer.Dispose();
    }
}
