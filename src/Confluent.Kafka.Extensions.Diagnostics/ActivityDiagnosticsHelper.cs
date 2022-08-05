using System.Diagnostics;
using System.Text;

namespace Confluent.Kafka.Extensions.Diagnostics;

internal static class ActivityDiagnosticsHelper
{
    private const string ActivitySourceName = "Confluent.Kafka.Extensions.Diagnostics";

    private static ActivitySource ActivitySource { get; } = new(ActivitySourceName);

    internal static Activity? Start<TKey, TValue>(TopicPartition partition, Message<TKey, TValue> message)
    {
        try
        {
            Activity? activity = ActivitySource.StartActivity("Confluent.Kafka.Produce", ActivityKind.Client,
                default(ActivityContext),
                new[]
                {
                    new KeyValuePair<string, object>("messaging.system", "kafka"),
                    new KeyValuePair<string, object>("messaging.destination", partition.Topic),
                    new KeyValuePair<string, object>("messaging.destination_kind", "topic"),
                    new KeyValuePair<string, object>("messaging.kafka.partition", partition.Partition.ToString())
                }!);

            if (activity == null) return null;

            if (activity.IsAllDataRequested)
            {
                if (message.Key != null)
                {
                    activity.SetTag("messaging.kafka.message_key", message.Key.ToString());
                }

                if (message.Value != null)
                {
                    int messagePayloadBytes = Encoding.UTF8.GetByteCount(message.Value.ToString()!);
                    activity.AddTag("messaging.message_payload_size_bytes", messagePayloadBytes.ToString());
                }
            }

            if (message.Headers == null)
            {
                message.Headers = new Headers();
            }

            if (activity.Id != null)
                message.Headers.Add("traceparent", Encoding.UTF8.GetBytes(activity.Id));

            var tracestateStr = activity.Context.TraceState;
            if (tracestateStr?.Length > 0)
            {
                message.Headers.Add("tracestate", Encoding.UTF8.GetBytes(tracestateStr));
            }

            return activity;
        }
        catch
        {
            // ignore
            return null;
        }
    }
}
