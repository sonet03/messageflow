using System.Collections.Generic;

namespace MessageFlow.Kafka
{
    public class MessageEnvelope<TPayload>
    {
        public MessageEnvelope(string key, TPayload payload, IDictionary<string, string>? headers = null)
        {
            Key = key;
            Payload = payload;
            Headers = headers ?? new Dictionary<string, string>();
        }
        
        public string Key { get; }
        public TPayload Payload { get; }
        public IDictionary<string, string> Headers { get; }
    }
}