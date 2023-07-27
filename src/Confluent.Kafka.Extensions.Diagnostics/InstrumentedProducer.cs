namespace Confluent.Kafka.Extensions.Diagnostics;

internal class InstrumentedProducer<TKey, TValue> : IProducer<TKey, TValue>
{
    private readonly IProducer<TKey, TValue> _producerImplementation;

    public InstrumentedProducer(IProducer<TKey, TValue> producer)
    {
        _producerImplementation = producer;
    }

    public void Dispose() => _producerImplementation.Dispose();

    public int AddBrokers(string brokers) => _producerImplementation.AddBrokers(brokers);

    public void SetSaslCredentials(string username, string password)
    {
        _producerImplementation.SetSaslCredentials(username, password);
    }

    public Handle Handle => _producerImplementation.Handle;

    public string Name => _producerImplementation.Name;

    public Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic, Message<TKey, TValue> message, CancellationToken cancellationToken = new CancellationToken()) =>
        ProduceAsync(new TopicPartition(topic, Partition.Any), message, cancellationToken);

    public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        TopicPartition topicPartition, Message<TKey, TValue> message,
        CancellationToken cancellationToken = new CancellationToken())
    {
        var activity = ActivityDiagnosticsHelper.StartProduceActivity(topicPartition, message);

        try
        {
            var result = await _producerImplementation.ProduceAsync(topicPartition, message, cancellationToken)
                .ConfigureAwait(false);

            if (activity != null)
            {
                ActivityDiagnosticsHelper.UpdateActivityTags(result, activity);
            }

            return result;
        }
        finally
        {
            activity?.Stop();
        }
    }

    public void Produce(
        string topic, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>>? deliveryHandler = null) =>
        Produce(new TopicPartition(topic, Partition.Any), message, deliveryHandler);

    public void Produce(
        TopicPartition topicPartition, Message<TKey, TValue> message,
        Action<DeliveryReport<TKey, TValue>>? deliveryHandler = null)
    {
        var activity = ActivityDiagnosticsHelper.StartProduceActivity(topicPartition, message);

        Action<DeliveryReport<TKey, TValue>>? handler = activity != null
            ? report =>
            {
                ActivityDiagnosticsHelper.UpdateActivityTags(report, activity);
                activity.Stop();
                deliveryHandler?.Invoke(report);
            }
            : deliveryHandler;

        _producerImplementation.Produce(topicPartition, message, handler);
    }

    public int Poll(TimeSpan timeout) => _producerImplementation.Poll(timeout);

    public int Flush(TimeSpan timeout) => _producerImplementation.Flush(timeout);

    public void Flush(CancellationToken cancellationToken = new CancellationToken()) =>
        _producerImplementation.Flush(cancellationToken);

    public void InitTransactions(TimeSpan timeout) => _producerImplementation.InitTransactions(timeout);

    public void BeginTransaction() => _producerImplementation.BeginTransaction();

    public void CommitTransaction(TimeSpan timeout) => _producerImplementation.CommitTransaction(timeout);

    public void CommitTransaction() => _producerImplementation.CommitTransaction();

    public void AbortTransaction(TimeSpan timeout) => _producerImplementation.AbortTransaction(timeout);

    public void AbortTransaction() => _producerImplementation.AbortTransaction();

    public void SendOffsetsToTransaction(
        IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout) =>
        _producerImplementation.SendOffsetsToTransaction(offsets, groupMetadata, timeout);
}
