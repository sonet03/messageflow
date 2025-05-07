using System.Collections.Generic;
using Confluent.Kafka;

namespace MessageFlow.Kafka
{
    public class MessageEnvelope<TPayload>
    {
        public MessageEnvelope(string key, TopicPartitionOffset topicPartitionOffset, TPayload payload,
            IDictionary<string, string>? headers = null)
        {
            Key = key;
            TopicPartitionOffset = topicPartitionOffset;
            Payload = payload;
            Headers = headers ?? new Dictionary<string, string>();
        }

        public string Key { get; }
        public TopicPartitionOffset TopicPartitionOffset { get; set; }
        public TPayload Payload { get; }
        public IDictionary<string, string> Headers { get; }
    }
}