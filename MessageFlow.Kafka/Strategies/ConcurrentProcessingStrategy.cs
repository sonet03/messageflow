using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Strategies
{
    public class ConcurrentProcessingStrategy<TMessage> : IProcessingStrategy<TMessage>
    {
        private readonly TransformBlock<MessageEnvelope<TMessage>, MessageEnvelope<TMessage>>[] _partitionedBlocks;
        private readonly TransformBlock<MessageEnvelope<TMessage>, MessageEnvelope<TMessage>> _runBlock;

        public ConcurrentProcessingStrategy(Func<TMessage, Task> handle, Action<TopicPartitionOffset> onCompleted, int maxConcurrency = 5)
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
                        await handle(m.Payload);
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

        private Task<MessageEnvelope<TMessage>> RouteToPartition(MessageEnvelope<TMessage> message)
        {
            return Task.FromResult(message);
        }

        public Task DispatchAsync(MessageEnvelope<TMessage> message)
        {
            return _runBlock.SendAsync(message);
        }
    }
} 