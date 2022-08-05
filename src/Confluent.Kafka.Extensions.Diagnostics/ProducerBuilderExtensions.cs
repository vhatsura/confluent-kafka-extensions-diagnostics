namespace Confluent.Kafka.Extensions.Diagnostics;

public static class ProducerBuilderExtensions
{
    public static IProducer<TKey, TValue> BuildWithInstrumentation<TKey, TValue>(
        this ProducerBuilder<TKey, TValue> producerBuilder)
    {
        return new InstrumentedProducer<TKey, TValue>(producerBuilder.Build());
    }
}
