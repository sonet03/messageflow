using Confluent.Kafka;
using MessageFlow.Kafka;
using MessageFlow.Kafka.Deserializers;
using Microsoft.Extensions.Logging;
using SharedLibrary;

var config = new ConsumerConfig
{
    BootstrapServers = "kafka:9092",
    GroupId = "my-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};

var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
var logger = loggerFactory.CreateLogger<KafkaMessageListener<TestMessage>>();

var listener = new KafkaMessageListener<TestMessage>(config,
    new JsonDeserializer<TestMessage>(),
    logger,
    "kafka-topic-test"
);

listener.Subscribe(async message =>
{
    Console.WriteLine($"[{Environment.GetEnvironmentVariable("CONSUMER_ID")}] Received: {message.Content}");
    await Task.Delay(Random.Shared.Next(300, 500));
});

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    listener.Dispose();
};

await Task.Delay(-1);