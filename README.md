# Confluent.Kafka.Extensions.Diagnostics

![GitHub Actions Badge](https://github.com/vhatsura/confluent-kafka-extensions-diagnostics/actions/workflows/continuous.integration.yml/badge.svg)
[![NuGet Badge](https://buildstats.info/nuget/Confluent.Kafka.Extensions.Diagnostics)](https://www.nuget.org/packages/Confluent.Kafka.Extensions.Diagnostics/)

The `Confluent.Kafka.Extensions.Diagnostics` package enables instrumentation of the `Confluent.Kafka` library
via [Activity API](https://docs.microsoft.com/en-us/dotnet/core/diagnostics/distributed-tracing-instrumentation-walkthroughs).

## Installation

```powershell
Install-Package Confluent.Kafka.Extensions.Diagnostics
```

## Usage

### Producer

Producer instrumentation is done via wrapper class and, for this reason, the producer usage is not needed to be rewritten. However,
to enable producer instrumentation, `BuildWithInstrumentation` method should be called on the producer builder instead of `Build`.
After that, all produce calls (sync and async) will be instrumented.

```csharp
using Confluent.Kafka;
using Confluent.Kafka.Extensions.Diagnostics;


using var producer =
    new ProducerBuilder<Null, string>(new ProducerConfig(new ClientConfig { BootstrapServers = "localhost:9092" }))
        .SetKeySerializer(Serializers.Null)
        .SetValueSerializer(Serializers.Utf8)
        .BuildWithInstrumentation();

await producer.ProduceAsync("topic", new Message<Null, string> { Value = "Hello World!" });

```

### Consumer

Unfortunately, consumer interface of `Confluent.Kafka` library is not very flexible. Therefore, the instrumentation is implemented
via an extension method on the consumer itself. For this reason, the consumer usage should be rewritten as follows:

```csharp
using Confluent.Kafka;
using Confluent.Kafka.Extensions.Diagnostics;

using var consumer = new ConsumerBuilder<Ignore, string>(
        new ConsumerConfig(new ClientConfig { BootstrapServers = "localhost:9092" })
        {
            GroupId = "group", AutoOffsetReset = AutoOffsetReset.Earliest
        })
    .SetValueDeserializer(Deserializers.Utf8)
    .Build();

consumer.Subscribe("topic");

try
{
    while (true)
    {
        try
        {
            consumer.ConsumeWithInstrumentation((result) =>
            {
                Console.WriteLine(result.Message.Value);
            }, 2000);
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Error occured: {e.Error.Reason}");
        }
    }
}
catch (OperationCanceledException)
{
    consumer.Close();
}
```
