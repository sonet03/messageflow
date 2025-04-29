using System;
using System.Threading.Tasks;

namespace MessageFlow
{
    public interface IMessageListener<TMessage>
    {
        void Subscribe(Func<TMessage, Task> handle);
    }
}