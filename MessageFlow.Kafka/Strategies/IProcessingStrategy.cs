using System.Threading.Tasks;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Strategies
{
    public interface IProcessingStrategy<in TMessage>
    {
        public Task DispatchAsync(TMessage consumeResult);
    }
}