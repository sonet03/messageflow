using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using MessageFlow.Kafka.Strategies;
using Microsoft.Extensions.Logging;

namespace MessageFlow.Kafka
{
    public class KafkaMessageListener<TMessage> : IMessageListener<TMessage>, IDisposable
    {
        private readonly IConsumer<string, byte[]> _consumer;
        private readonly Func<byte[], TMessage> _deserializer;
        
        private readonly ConcurrentBag<IProcessingStrategy<TMessage>> _subscribers = new ConcurrentBag<IProcessingStrategy<TMessage>>();
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly Task _listenerTask;
        
        private readonly ILogger<KafkaMessageListener<TMessage>> _logger;

        protected KafkaMessageListener(ConsumerConfig consumerConfig, Func<byte[], TMessage> deserializer, ILogger<KafkaMessageListener<TMessage>> logger)
        {
            _deserializer = deserializer;
            _logger = logger;

            _consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
            _consumer.Subscribe(GetTopicName());
            _listenerTask = Task.Run(Listen);
        }

        private string GetTopicName()
        {
            return typeof(TMessage).Name;
        }

        public void Subscribe(Func<TMessage, Task> handle)
        {
            _subscribers.Add(new SequentialProcessingStrategy<TMessage>(handle));
        }

        private async Task Listen()
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(_cts.Token);

                    var message = _deserializer(consumeResult.Message.Value);
                    var messageEnvelope = new MessageEnvelope<TMessage>(consumeResult.Message.Key, message, GetHeaders(consumeResult.Message.Headers));
                    foreach (var subscriber in _subscribers)
                    {
                        await subscriber.DispatchAsync(messageEnvelope);
                    }

                    _consumer.Commit(consumeResult);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex,"Error consuming message");
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        private Dictionary<string, string> GetHeaders(Headers headers)
        {
            var result = new Dictionary<string, string>();
            
            foreach (var header in headers)
            {
                var key = header.Key;
                var value = header.GetValueBytes() != null
                    ? Encoding.UTF8.GetString(header.GetValueBytes())
                    : null;

                if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(value))
                {
                    result[key] = value;
                }

            }

            return result;
        }

        public void Dispose()
        {
            _cts.Cancel();
            _consumer.Close();
            _consumer.Dispose();
        }
    }
}