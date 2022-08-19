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
                $"produce kafka.{partition.Topic}{partition.Partition.ToString()}", ActivityKind.Producer,
                default(ActivityContext), ActivityTags(partition)!);

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

    internal static Activity? StartConsumeActivity<TKey, TValue>(TopicPartition partition,
        Message<TKey, TValue> message)
    {
        try
        {
            var activity = ActivitySource.CreateActivity(
                $"consume kafka.{partition.Topic}{partition.Partition.ToString()}", ActivityKind.Consumer,
                default(ActivityContext), ActivityTags(partition)!);

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
            activity.SetTag("messaging.kafka.message_key", message.Key.ToString());
        }
    }

    private static IEnumerable<KeyValuePair<string, object>> ActivityTags(TopicPartition partition)
    {
        return new[]
        {
            new KeyValuePair<string, object>("messaging.system", "kafka"),
            new KeyValuePair<string, object>("messaging.destination", partition.Topic),
            new KeyValuePair<string, object>("messaging.destination_kind", "topic"), new KeyValuePair<string, object>(
                "messaging.kafka.partition",
                partition.Partition.ToString())
        };
    }
}
