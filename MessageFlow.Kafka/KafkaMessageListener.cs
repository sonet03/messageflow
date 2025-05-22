using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using Confluent.Kafka;
using MessageFlow.Kafka.Strategies;
using Microsoft.Extensions.Logging;

namespace MessageFlow.Kafka
{
    public class KafkaMessageListener<TMessage> : IMessageListener<TMessage>, IDisposable
    {
        private readonly IConsumer<string, TMessage> _consumer;
        
        //private readonly ConcurrentBag<IProcessingStrategy<TMessage>> _subscribers = new ConcurrentBag<IProcessingStrategy<TMessage>>();
        private IProcessingStrategy<TMessage> _processingStrategy;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly Task _listenerTask;

        private readonly ConcurrentDictionary<TopicPartition, OffsetCommitCoordinator> _coordinators =
            new ConcurrentDictionary<TopicPartition, OffsetCommitCoordinator>();
        
        private readonly ILogger<KafkaMessageListener<TMessage>> _logger;

        public KafkaMessageListener(ConsumerConfig consumerConfig, IDeserializer<TMessage> deserializer, ILogger<KafkaMessageListener<TMessage>> logger, string? topic = null)
        {
            _logger = logger;

            _consumer = new ConsumerBuilder<string, TMessage>(consumerConfig)
                .SetPartitionsAssignedHandler(OnPartitionsAssigned)
                .SetPartitionsRevokedHandler(OnPartitionsRevoked)
                .SetValueDeserializer(deserializer)
                .Build();
            
            _consumer.Subscribe(topic ?? GetTopicName());
            _listenerTask = Task.Run(Listen);
        }

        private string GetTopicName()
        {
            return typeof(TMessage).Name;
        }

        public void Subscribe(Func<TMessage, Task> handle)
        {
            _processingStrategy = new SequentialProcessingStrategy<TMessage>(handle, OnCompleted);
        }

        private async Task Listen()
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(_cts.Token);

                    var messageEnvelope = new MessageEnvelope<TMessage>(consumeResult.Message.Key,
                        consumeResult.TopicPartitionOffset, consumeResult.Message.Value,
                        GetHeaders(consumeResult.Message.Headers));
                    
                    await _processingStrategy.DispatchAsync(messageEnvelope);
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

        private void OnCompleted(TopicPartitionOffset offset)
        {
            _coordinators[offset.TopicPartition].Acknowledge(offset);
        }

        private void OnPartitionsAssigned(IConsumer<string, TMessage> consumer, List<TopicPartition> partitions)
        {
            foreach (var tp in partitions)
            {
                long startingOffset = consumer.Position(tp);

                _coordinators[tp] = new OffsetCommitCoordinator(
                    minOffset: startingOffset,
                    commitAction: nextOffset => consumer.Commit(new[] { new TopicPartitionOffset(tp, new Offset(nextOffset)) })
                );
            }
        }

        private void OnPartitionsRevoked(IConsumer<string, TMessage> consumer, List<TopicPartitionOffset> partitions)
        {
            foreach (var tp in partitions)
            {
                _coordinators.TryRemove(tp.TopicPartition, out _);
            }
        }
    }
}