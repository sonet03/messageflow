using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Strategies
{
    public class BatchProcessingStrategy<TMessage> : IProcessingStrategy<TMessage>
    {
        private readonly BatchBlock<MessageEnvelope<TMessage>> _batchBlock;
        private readonly TransformBlock<IEnumerable<MessageEnvelope<TMessage>>, IEnumerable<MessageEnvelope<TMessage>>> _runBlock;

        public BatchProcessingStrategy(Func<IEnumerable<MessageEnvelope<TMessage>>, Task> handle,
            Action<TopicPartitionOffset> onCompleted, int batchSize = 100, int batchTimeoutMs = 1000)
        {
            _batchBlock = new BatchBlock<MessageEnvelope<TMessage>>(batchSize,
                new GroupingDataflowBlockOptions
                {
                    BoundedCapacity = 100,
                    EnsureOrdered = true
                });
            var options =
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 1,
                    BoundedCapacity = 100
                };
            _runBlock =
                new TransformBlock<IEnumerable<MessageEnvelope<TMessage>>, IEnumerable<MessageEnvelope<TMessage>>>(
                    async m =>
                    {
                        var messageEnvelopes = m.ToList();
                        await handle(messageEnvelopes);
                        return messageEnvelopes;
                    }, options);
            var completedBlock = new ActionBlock<IEnumerable<MessageEnvelope<TMessage>>>(messages =>
            {
                foreach (var m in messages)
                {
                    onCompleted(m.TopicPartitionOffset);
                }
            }, options);

            _batchBlock.LinkTo(_runBlock);
            _runBlock.LinkTo(completedBlock);
        }

        public Task DispatchAsync(MessageEnvelope<TMessage> message)
        {
            return _batchBlock.SendAsync(message);
        }
    }
} 