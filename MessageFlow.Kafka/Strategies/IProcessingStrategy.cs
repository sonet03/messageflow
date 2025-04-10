using System.Threading.Tasks;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Strategies
{
    public interface IProcessingStrategy
    {
        public Task Execute(ConsumeResult<string, string> consumeResult);
    }
}