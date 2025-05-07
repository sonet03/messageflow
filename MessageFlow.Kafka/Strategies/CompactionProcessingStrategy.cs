using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Strategies
{
    public class CompactionProcessingStrategy<TMessage> : IProcessingStrategy<TMessage>
    {
        private readonly TransformBlock<MessageEnvelope<TMessage>, MessageEnvelope<TMessage>>[] _partitionedBlocks;
        private readonly TransformBlock<MessageEnvelope<TMessage>, MessageEnvelope<TMessage>> _runBlock;
        
        private readonly ConcurrentDictionary<string, Offset> _latestOffset = new ConcurrentDictionary<string, Offset>();

        public CompactionProcessingStrategy(Func<TMessage, Task> handle, Action<TopicPartitionOffset> onCompleted, int maxConcurrency = 5)
        {
            _runBlock = new TransformBlock<MessageEnvelope<TMessage>, MessageEnvelope<TMessage>>(RouteToPartition, new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = maxConcurrency,
                BoundedCapacity = 100,
                EnsureOrdered = true
            });
            
            var completedBlock = new ActionBlock<MessageEnvelope<TMessage>>(m => onCompleted(m.TopicPartitionOffset), new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = maxConcurrency,
                BoundedCapacity = 100
            });
            
            _partitionedBlocks = new TransformBlock<MessageEnvelope<TMessage>, MessageEnvelope<TMessage>>[maxConcurrency];
            
            for (var i = 0; i < maxConcurrency; i++)
            {
                _partitionedBlocks[i] = new TransformBlock<MessageEnvelope<TMessage>, MessageEnvelope<TMessage>>(
                    async m =>
                    {
                        if (IsLatest(m))
                        {
                            await handle(m.Payload);
                        }
                        
                        return m;
                    },
                    new ExecutionDataflowBlockOptions
                    {
                        MaxDegreeOfParallelism = 1,
                        BoundedCapacity = 100
                    });

                var index = i;
                _runBlock.LinkTo(_partitionedBlocks[i], m => Math.Abs(m.Key.GetHashCode()) % maxConcurrency == index);
                _partitionedBlocks[i].LinkTo(completedBlock);
            }
        }

        private bool IsLatest(MessageEnvelope<TMessage> message)
        {
            return _latestOffset.TryGetValue(message.Key, out var latestOffset) &&
                   latestOffset == message.TopicPartitionOffset.Offset;
            // TODO: possible problem with high load when we can't process message
            // TODO: Do not commit immediately if skip
        }

        private Task<MessageEnvelope<TMessage>> RouteToPartition(MessageEnvelope<TMessage> message)
        {
            return Task.FromResult(message);
        }

        public Task DispatchAsync(MessageEnvelope<TMessage> message)
        {
            _latestOffset.AddOrUpdate(message.Key, message.TopicPartitionOffset.Offset, (a, b) => message.TopicPartitionOffset.Offset);
            return _runBlock.SendAsync(message);
        }
    }
} 