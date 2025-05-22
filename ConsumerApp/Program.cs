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
var logger = loggerFactory.CreateLogger<KafkaMessageListener<OrderMessage>>();

var listener = new KafkaMessageListener<OrderMessage>(config,
    new JsonDeserializer<OrderMessage>(),
    logger,
    "kafka-topic-test"
);

listener.Subscribe(async message =>
{
    await Task.Delay(300);
    var now = DateTimeOffset.UtcNow;
    var formatted = now.ToString("HH:mm:ss.fff");
    Console.WriteLine($"{formatted} | {message.UserId} | {message.OrderId}");
});

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    listener.Dispose();
};

await Task.Delay(-1);