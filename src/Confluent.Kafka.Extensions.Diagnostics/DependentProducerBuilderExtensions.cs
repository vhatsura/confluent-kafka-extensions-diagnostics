namespace Confluent.Kafka.Extensions.Diagnostics;

/// <summary>
///     Extension methods for <see cref="DependentProducerBuilder{TKey,TValue}" />.
/// </summary>
public static class DependentProducerBuilderExtensions
{
    /// <summary>
    ///     Builds a new instrumented instance of producer.
    /// </summary>
    public static IProducer<TKey, TValue> BuildWithInstrumentation<TKey, TValue>(
        this DependentProducerBuilder<TKey, TValue> producerBuilder
    )
    {
        if (producerBuilder == null) throw new ArgumentNullException(nameof(producerBuilder));

        return new InstrumentedProducer<TKey, TValue>(producerBuilder.Build());
    }
}
