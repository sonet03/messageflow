using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace MessageFlow.Kafka
{
    public class KafkaMessagePublisher<TMessage> : IMessagePublisher<TMessage>, IDisposable
    {
        private readonly string _topic;
        private readonly Func<TMessage, byte[]> _serializer;
        private readonly ILogger<KafkaMessagePublisher<TMessage>> _logger;
        private readonly IProducer<string, byte[]> _producer;
        
        public KafkaMessagePublisher(ProducerConfig producerConfig, string topic, Func<TMessage, byte[]> serializer, ILogger<KafkaMessagePublisher<TMessage>> logger)
        {
            _producer = new ProducerBuilder<string, byte[]>(producerConfig).Build();
            _topic = topic;
            _serializer = serializer;
            _logger = logger;
        }
        
        public async Task PublishAsync(string key, TMessage message)
        {
            try
            {
                var result =
                    await _producer.ProduceAsync(_topic,
                        new Message<string, byte[]> { Key = key, Value = _serializer(message) });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to publish message for message with {Key}", key);
            }
        }
        
        public void Dispose()
        {
            _producer.Flush(TimeSpan.FromSeconds(5));
            _producer.Dispose();
        }
    }
}