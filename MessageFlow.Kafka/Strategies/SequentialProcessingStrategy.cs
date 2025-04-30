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
        private readonly ActionBlock<MessageEnvelope<TMessage>> _runBlock;

        public SequentialProcessingStrategy(Func<TMessage, Task> handle)
        {
            _runBlock = new ActionBlock<MessageEnvelope<TMessage>>(m => handle(m.Payload), new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = 100 // TODO: make configurable
            });
        }

        public Task DispatchAsync(MessageEnvelope<TMessage> message)
        {
            return _runBlock.SendAsync(message);
        }
    }
} 