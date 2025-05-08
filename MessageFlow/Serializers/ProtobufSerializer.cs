using System;
using System.IO;
using System.Runtime.Serialization;
using Confluent.Kafka;
using Google.Protobuf;

namespace MessageFlow.Serializers
{
    public class ProtobufSerializer<T> : ISerializer<T> where T : IMessage<T>
    {
        public byte[]? Serialize(T data, SerializationContext context)
        {
            if (data == null)
            {
                return null;
            }

            try
            {
                using var stream = new MemoryStream();
                data.WriteTo(stream);
                return stream.ToArray();
            }
            catch (Exception ex)
            {
                throw new SerializationException($"Error serializing {typeof(T).Name}: {ex.Message}", ex);
            }
        }
    }
}