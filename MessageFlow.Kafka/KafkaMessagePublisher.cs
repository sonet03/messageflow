using System;

namespace MessageFlow.Kafka
{
    public class KafkaMessagePublisher<TMessage> : IMessagePublisher<TMessage>, IDisposable
    {
        public void Dispose()
        {
        }
    }
}