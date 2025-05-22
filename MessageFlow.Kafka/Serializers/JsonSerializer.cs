using System;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace MessageFlow.Kafka.Serializers
{
    public class JsonSerializer<T> : ISerializer<T>
    {
        private readonly JsonSerializerOptions _options;

        public JsonSerializer(JsonSerializerOptions? options = null)
        {
            _options = options ?? new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false
            };
        }

        public byte[]? Serialize(T data, SerializationContext context)
        {
            if (data == null)
            {
                return null;
            }

            try
            {
                var json = JsonSerializer.Serialize(data, _options);
                return Encoding.UTF8.GetBytes(json);
            }
            catch (Exception ex)
            {
                throw new SerializationException($"Error serializing {typeof(T).Name}: {ex.Message}", ex);
            }
        }
    }
}