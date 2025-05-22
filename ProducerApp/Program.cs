using Confluent.Kafka;
using MessageFlow.Kafka;
using MessageFlow.Kafka.Serializers;
using Microsoft.Extensions.Logging;
using SharedLibrary;

var config = new ProducerConfig
{
    BootstrapServers = "kafka:9092"
};

var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
var logger = loggerFactory.CreateLogger<KafkaMessagePublisher<TestMessage>>();

var publisher = new KafkaMessagePublisher<TestMessage>(
    config,
    topic: "kafka-topic-test",
    serializer: new JsonSerializer<TestMessage>(),
    logger
);

for (var i = 0; i < 10; i++)
{
    await publisher.PublishAsync(i.ToString(), new TestMessage { Content = $"Hello {i}" });
    Console.WriteLine($"Published message {i}");
    await Task.Delay(100);
}

publisher.Dispose();