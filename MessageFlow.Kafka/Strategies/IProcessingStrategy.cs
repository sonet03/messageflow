using System.Threading.Tasks;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Strategies
{
    public interface IProcessingStrategy<TMessage>
    {
        public Task DispatchAsync(MessageEnvelope<TMessage> message);
    }
}