using System.Threading.Tasks;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Strategies
{
    public class ConcurrentProcessingStrategy<TMessage> : IProcessingStrategy<TMessage>
    {
        private readonly int _maxConcurrency;

        public ConcurrentProcessingStrategy(int maxConcurrency = 5)
        {
            _maxConcurrency = maxConcurrency;
        }

        public Task DispatchAsync(TMessage message)
        {
            return Task.CompletedTask;
        }
    }
} 