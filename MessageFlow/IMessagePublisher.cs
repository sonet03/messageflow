using System.Threading.Tasks;

namespace MessageFlow
{
    public interface IMessagePublisher<TMessage>
    {
        public Task PublishAsync(string key, TMessage message);
    }
}