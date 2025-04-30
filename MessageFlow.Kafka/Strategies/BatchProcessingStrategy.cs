using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Strategies
{
    public class BatchProcessingStrategy<TMessage> : IProcessingStrategy<TMessage>
    {
        private readonly BatchBlock<MessageEnvelope<TMessage>> _batchBlock;
        private readonly ActionBlock<IEnumerable<MessageEnvelope<TMessage>>> _runBlock;

        public BatchProcessingStrategy(Func<IEnumerable<MessageEnvelope<TMessage>>, Task> handle, int batchSize = 100, int batchTimeoutMs = 1000)
        {
            _batchBlock = new BatchBlock<MessageEnvelope<TMessage>>(batchSize,
                new GroupingDataflowBlockOptions
                {
                    BoundedCapacity = 100,
                    EnsureOrdered = true
                });
            _runBlock = new ActionBlock<IEnumerable<MessageEnvelope<TMessage>>>(handle,
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 1,
                    BoundedCapacity = 100
                });

            _batchBlock.LinkTo(_runBlock);
        }

        public Task DispatchAsync(MessageEnvelope<TMessage> message)
        {
            return _batchBlock.SendAsync(message);
        }
    }
} 