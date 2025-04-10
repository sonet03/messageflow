using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Strategies
{
    public class BatchProcessingStrategy : IProcessingStrategy
    {
        private readonly int _batchSize;
        private readonly int _batchTimeoutMs;

        public BatchProcessingStrategy(int batchSize = 100, int batchTimeoutMs = 1000)
        {
            _batchSize = batchSize;
            _batchTimeoutMs = batchTimeoutMs;
        }

        public Task Execute(ConsumeResult<string, string> consumeResult)
        {
            
            return Task.CompletedTask;
        }
    }
} 