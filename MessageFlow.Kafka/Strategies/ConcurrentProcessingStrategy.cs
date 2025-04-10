using System.Threading.Tasks;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Strategies
{
    public class ConcurrentProcessingStrategy : IProcessingStrategy
    {
        private readonly int _maxConcurrency;

        public ConcurrentProcessingStrategy(int maxConcurrency = 5)
        {
            _maxConcurrency = maxConcurrency;
        }

        public Task Execute(ConsumeResult<string, string> consumeResult)
        {
            return Task.CompletedTask;
        }
    }
} 