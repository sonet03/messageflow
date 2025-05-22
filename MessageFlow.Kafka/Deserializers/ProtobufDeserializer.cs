using System;
using System.Runtime.Serialization;
using Confluent.Kafka;
using Google.Protobuf;

namespace MessageFlow.Kafka.Deserializers
{
    public class ProtobufDeserializer<T> : IDeserializer<T> where T : IMessage<T>, new()
    {
        private readonly MessageParser<T> _parser;

        public ProtobufDeserializer()
        {
            var instance = new T();
            _parser = (MessageParser<T>)instance.Descriptor.Parser;
        }

        public ProtobufDeserializer(MessageParser<T> parser)
        {
            _parser = parser ?? throw new ArgumentNullException(nameof(parser));
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull || data.Length == 0)
            {
                return default;
            }

            try
            {
                return _parser.ParseFrom(data.ToArray());
            }
            catch (Exception ex)
            {
                throw new SerializationException($"Error deserializing {typeof(T).Name}: {ex.Message}", ex);
            }
        }
    }
}