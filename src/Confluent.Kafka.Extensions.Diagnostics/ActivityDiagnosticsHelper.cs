using System.Diagnostics;
using System.Text;

namespace Confluent.Kafka.Extensions.Diagnostics;

internal static class ActivityDiagnosticsHelper
{
    private const string ActivitySourceName = "Confluent.Kafka.Extensions.Diagnostics";
    private const string TraceParentHeaderName = "traceparent";
    private const string TraceStateHeaderName = "tracestate";

    private static ActivitySource ActivitySource { get; } = new(ActivitySourceName);

    internal static Activity? StartProduceActivity<TKey, TValue>(TopicPartition partition,
        Message<TKey, TValue> message)
    {
        try
        {
            Activity? activity = ActivitySource.StartActivity(
                $"{partition.Topic} publish", ActivityKind.Producer,
                default(ActivityContext), ProducerActivityTags(partition));

            if (activity == null)
                return null;

            if (activity.IsAllDataRequested)
            {
                SetActivityTags(activity, message);
            }

            if (message.Headers == null)
            {
                message.Headers = new Headers();
            }

            if (activity.Id != null)
                message.Headers.Add(TraceParentHeaderName, Encoding.UTF8.GetBytes(activity.Id));

            var tracestateStr = activity.Context.TraceState;
            if (tracestateStr?.Length > 0)
            {
                message.Headers.Add(TraceStateHeaderName, Encoding.UTF8.GetBytes(tracestateStr));
            }

            return activity;
        }
        catch
        {
            // ignore
            return null;
        }
    }

    internal static void UpdateActivityTags<TKey, TValue>(DeliveryResult<TKey, TValue> deliveryResult,
        Activity activity)
    {
        try
        {
            var activityStatus = deliveryResult.Status switch
            {
                PersistenceStatus.Persisted => ActivityStatusCode.Ok,
                PersistenceStatus.NotPersisted => ActivityStatusCode.Error,
                _ => ActivityStatusCode.Unset
            };

            activity.SetStatus(activityStatus);
            if (activityStatus == ActivityStatusCode.Ok)
            {
                activity.SetTag("messaging.kafka.destination.partition", deliveryResult.Partition.Value.ToString());
                activity.SetTag("messaging.kafka.message.offset", deliveryResult.Offset.Value.ToString());
            }
        }
        catch
        {
            // ignore
        }
    }

    internal static Activity? StartConsumeActivity<TKey, TValue>(ConsumeResult<TKey, TValue> consumerResult,
        string memberId)
    {
        try
        {
            var message = consumerResult.Message;
            var activity = ActivitySource.CreateActivity(
                $"{consumerResult.Topic} process", ActivityKind.Consumer,
                default(ActivityContext), ConsumerActivityTags(consumerResult, memberId));

            if (activity != null)
            {
                var traceParentHeader = message.Headers?.FirstOrDefault(x => x.Key == TraceParentHeaderName);
                var traceStateHeader = message.Headers?.FirstOrDefault(x => x.Key == TraceStateHeaderName);

                var traceParent = traceParentHeader != null
                    ? Encoding.UTF8.GetString(traceParentHeader.GetValueBytes())
                    : null;
                var traceState = traceStateHeader != null
                    ? Encoding.UTF8.GetString(traceStateHeader.GetValueBytes())
                    : null;

                if (ActivityContext.TryParse(traceParent, traceState, out var activityContext))
                {
                    activity.SetParentId(activityContext.TraceId, activityContext.SpanId, activityContext.TraceFlags);
                    activity.TraceStateString = activityContext.TraceState;
                }

                if (activity.IsAllDataRequested)
                {
                    SetActivityTags(activity, message);
                }

                activity.Start();
            }


            return activity;
        }
        catch
        {
            // ignore
            return null;
        }
    }

    private static void SetActivityTags<TKey, TValue>(Activity activity, Message<TKey, TValue> message)
    {
        if (message.Key != null)
        {
            activity.SetTag("messaging.kafka.message.key", message.Key.ToString());
        }
    }

    private static IEnumerable<KeyValuePair<string, object?>> ProducerActivityTags(TopicPartition partition)
    {
        var list = ActivityTags(partition, "publish");

        list.Add(new KeyValuePair<string, object?>("messaging.destination.kind", "topic"));
        list.Add(new KeyValuePair<string, object?>("messaging.destination.name", partition.Topic));

        return list;
    }

    private static IEnumerable<KeyValuePair<string, object?>> ConsumerActivityTags<TKey, TValue>(
        ConsumeResult<TKey, TValue> consumerResult, string memberId)
    {
        IList<KeyValuePair<string, object?>> list = ActivityTags(consumerResult.TopicPartition, "process");

        // messaging.consumer.id - For Kafka, set it to {messaging.kafka.consumer.group} - {messaging.kafka.client_id},
        // if both are present, or only messaging.kafka.consumer.group
        list.Add(new("messaging.source.kind", "topic"));
        list.Add(new("messaging.source.name", consumerResult.Topic));
        list.Add(new("messaging.kafka.source.partition", consumerResult.Partition.Value.ToString()));
        list.Add(new("messaging.kafka.message.offset", consumerResult.Offset.Value.ToString()));
        list.Add(new("messaging.kafka.client_id", memberId));

        // messaging.kafka.consumer.group - there is no way to access this information from the consumer

        return list;
    }

    private static IList<KeyValuePair<string, object?>> ActivityTags(TopicPartition partition, string operation)
    {
        return new List<KeyValuePair<string, object?>>()
        {
            new("messaging.system", "kafka"), new("messaging.operation", operation)
        };
    }
}
