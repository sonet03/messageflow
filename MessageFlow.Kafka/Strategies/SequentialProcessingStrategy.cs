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
        private readonly ActionBlock<TMessage> _runBlock;

        public SequentialProcessingStrategy(Func<TMessage, Task> handle)
        {
            _runBlock = new ActionBlock<TMessage>(handle, new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = 100 // TODO: make configurable
            });
        }

        public Task DispatchAsync(TMessage message)
        {
            return _runBlock.SendAsync(message);
        }
    }
} 