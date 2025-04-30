using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace MessageFlow.Kafka.Strategies
{
    public class ConcurrentProcessingStrategy<TMessage> : IProcessingStrategy<TMessage>
    {
        private readonly ActionBlock<MessageEnvelope<TMessage>>[] _partitionedBlocks;
        private readonly TransformBlock<MessageEnvelope<TMessage>, MessageEnvelope<TMessage>> _runBlock;

        public ConcurrentProcessingStrategy(Func<TMessage, Task> handle, int maxConcurrency = 5)
        {
            /*_runBlock = new TransformBlock<MessageEnvelope<TMessage>, MessageEnvelope<TMessage>>(RouteToPartition, new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = maxConcurrency,
                BoundedCapacity = 100
            });*/
            _partitionedBlocks = new ActionBlock<MessageEnvelope<TMessage>>[maxConcurrency];
            
            for (var i = 0; i < maxConcurrency; i++)
            {
                _partitionedBlocks[i] = new ActionBlock<MessageEnvelope<TMessage>>(
                    m => handle(m.Payload),
                    new ExecutionDataflowBlockOptions
                    {
                        MaxDegreeOfParallelism = 1,
                        BoundedCapacity = 100
                    });

                // _runBlock.LinkTo(_partitionedBlocks[i], m => Math.Abs(m.Key.GetHashCode()) % maxConcurrency == i);
            }
        }

        /*
        private async Task<MessageEnvelope<TMessage>> RouteToPartition(MessageEnvelope<TMessage> message)
        {
            var index = Math.Abs(message.Key.GetHashCode()) % _partitionedBlocks.Length; // TODO: Chose better partition algorithm
            return message;
        }
        */

        private Task RouteToPartition(MessageEnvelope<TMessage> message)
        {
            var index = Math.Abs(message.Key.GetHashCode()) % _partitionedBlocks.Length; // TODO: Chose better partition algorithm
            return _partitionedBlocks[index].SendAsync(message);
        }

        public Task DispatchAsync(MessageEnvelope<TMessage> message)
        {
            return RouteToPartition(message);
        }
    }
} 