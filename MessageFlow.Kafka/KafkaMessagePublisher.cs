using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace MessageFlow.Kafka
{
    public class KafkaMessagePublisher<TMessage> : IMessagePublisher<TMessage>, IDisposable
    {
        private readonly string _topic;
        private readonly ILogger<KafkaMessagePublisher<TMessage>> _logger;
        private readonly IProducer<string, TMessage> _producer;
        
        public KafkaMessagePublisher(ProducerConfig producerConfig, string topic, ISerializer<TMessage> serializer, ILogger<KafkaMessagePublisher<TMessage>> logger)
        {
            _producer = new ProducerBuilder<string, TMessage>(producerConfig).SetValueSerializer(serializer).Build();
            _topic = topic;
            _logger = logger;
        }
        
        public async Task PublishAsync(string key, TMessage message)
        {
            try
            {
                var result =
                    await _producer.ProduceAsync(_topic,
                        new Message<string, TMessage> { Key = key, Value = message });
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