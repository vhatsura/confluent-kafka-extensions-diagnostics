namespace Confluent.Kafka.Extensions.Diagnostics;

/// <summary>
///     Extension methods for <see cref="IConsumer{TKey,TValue}" />.
/// </summary>
public static class ConsumerExtensions
{
    /// <summary>
    ///     Consumes a message from the topic with instrumentation.
    /// </summary>
    public static async Task ConsumeWithInstrumentation<TKey, TValue>(this IConsumer<TKey, TValue> consumer,
        Func<ConsumeResult<TKey, TValue>, CancellationToken, Task> action, CancellationToken cancellationToken)
    {
        var result = consumer.Consume(cancellationToken);

        var activity = ActivityDiagnosticsHelper.StartConsumeActivity(result.TopicPartition, result.Message);

        try
        {
            await action(result, cancellationToken);
        }
        finally
        {
            activity?.Stop();
        }
    }

    /// <summary>
    ///     Consumes a message from the topic with instrumentation.
    /// </summary>
    public static void ConsumeWithInstrumentation<TKey, TValue>(this IConsumer<TKey, TValue> consumer,
        Action<ConsumeResult<TKey, TValue>> action, int millisecondsTimeout)
    {
        var result = consumer.Consume(millisecondsTimeout);

        var activity = ActivityDiagnosticsHelper.StartConsumeActivity(result.TopicPartition, result.Message);

        try
        {
            action(result);
        }
        finally
        {
            activity?.Stop();
        }
    }
}
