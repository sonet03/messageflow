using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Strategies
{
    public class SequentialProcessingStrategy<TMessage> : IProcessingStrategy<TMessage>
    {
        private readonly TransformBlock<MessageEnvelope<TMessage>, MessageEnvelope<TMessage>> _runBlock;

        public SequentialProcessingStrategy(Func<TMessage, Task> handle, Action<TopicPartitionOffset> onCompleted)
        {
            var options = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = 100 // TODO: make configurable
            };

            _runBlock = new TransformBlock<MessageEnvelope<TMessage>, MessageEnvelope<TMessage>>(async m =>
            {
                await handle(m.Payload);
                return m;
            }, options);
            
            var completedBlock = new ActionBlock<MessageEnvelope<TMessage>>(m => onCompleted(m.TopicPartitionOffset), options);
            
            _runBlock.LinkTo(completedBlock);
        }

        public Task DispatchAsync(MessageEnvelope<TMessage> message)
        {
            return _runBlock.SendAsync(message);
        }
    }
} 