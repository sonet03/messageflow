using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Strategies
{
    public class SequentialProcessingStrategy : IProcessingStrategy
    {
        public Task Execute(ConsumeResult<string, string> consumeResult)
        {
            return Task.CompletedTask;
        }
    }
} 