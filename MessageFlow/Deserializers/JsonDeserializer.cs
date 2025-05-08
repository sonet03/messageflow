using System;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace MessageFlow.Deserializers
{
    public class JsonDeserializer<T> : IDeserializer<T>
    {
        private readonly JsonSerializerOptions _options;

        public JsonDeserializer(JsonSerializerOptions? options = null)
        {
            _options = options ?? new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                PropertyNameCaseInsensitive = true
            };
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull || data.Length == 0)
            {
                return default;
            }

            try
            {
                var json = Encoding.UTF8.GetString(data);
                return JsonSerializer.Deserialize<T>(json, _options);
            }
            catch (Exception ex)
            {
                throw new SerializationException($"Error deserializing {typeof(T).Name}: {ex.Message}", ex);
            }
        }
    }
}