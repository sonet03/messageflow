using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Strategies
{
    public class BatchProcessingStrategy<TMessage> : IProcessingStrategy<TMessage>
    {
        private readonly int _batchSize;
        private readonly int _batchTimeoutMs;

        public BatchProcessingStrategy(int batchSize = 100, int batchTimeoutMs = 1000)
        {
            _batchSize = batchSize;
            _batchTimeoutMs = batchTimeoutMs;
        }

        public Task DispatchAsync(TMessage message)
        {
            
            return Task.CompletedTask;
        }
    }
} 