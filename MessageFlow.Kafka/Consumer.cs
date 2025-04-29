using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Confluent.Kafka;
using MessageFlow.Kafka.Strategies;
using Microsoft.Extensions.Logging;

namespace MessageFlow.Kafka
{
    public class KafkaMessageListener<TMessage> : IMessageListener<TMessage>, IDisposable
    {
        private readonly IConsumer<string, string> _consumer;
        private readonly Func<string, TMessage> _deserializer;
        
        private readonly ConcurrentBag<IProcessingStrategy<TMessage>> _subscribers = new ConcurrentBag<IProcessingStrategy<TMessage>>();
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly Task _listenerTask;
        
        private readonly ILogger<KafkaMessageListener<TMessage>> _logger;

        protected KafkaMessageListener(ConsumerConfig consumerConfig, Func<string, TMessage> deserializer, ILogger<KafkaMessageListener<TMessage>> logger)
        {
            _deserializer = deserializer;
            _logger = logger;

            _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
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
                    foreach (var subscriber in _subscribers)
                    {
                        await subscriber.DispatchAsync(message);
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

        public void Dispose()
        {
            _cts.Cancel();
            _consumer.Close();
            _consumer.Dispose();
        }
    }
}