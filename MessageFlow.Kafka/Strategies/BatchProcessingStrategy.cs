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
        public Task DispatchAsync(MessageEnvelope<TMessage> message)
        {
            return Task.CompletedTask;
        }
    }
} 