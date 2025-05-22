using Confluent.Kafka;
using MessageFlow.Kafka;
using MessageFlow.Kafka.Serializers;
using Microsoft.Extensions.Logging;
using SharedLibrary;

var config = new ProducerConfig
{
    BootstrapServers = "kafka:9092",
    Partitioner = Partitioner.Murmur2
};

var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
var logger = loggerFactory.CreateLogger<KafkaMessagePublisher<OrderMessage>>();

var publisher = new KafkaMessagePublisher<OrderMessage>(
    config,
    topic: "kafka-topic-test",
    serializer: new JsonSerializer<OrderMessage>(),
    logger
);

OrderMessage GenerateOrderMessage(string userId, int orderId)
{
    return new OrderMessage(
        OrderId: orderId.ToString(),
        UserId: userId,
        ProductId: $"product_{Random.Shared.Next(1, 100)}",
        Quantity: Random.Shared.Next(1, 10),
        Price: Math.Round((decimal)(Random.Shared.NextDouble() * 100), 2),
        Timestamp: DateTime.UtcNow
    );
}

var totalMessages = int.TryParse(Environment.GetEnvironmentVariable("TOTAL_MESSAGES"), out var n) ? n : 1000;
var tasks = new List<Task>();

for (int userIndex = 0; userIndex < 16; userIndex++)
{
    var userId = $"user_{userIndex}";
    tasks.Add(Task.Run(async () =>
    {
        for (int i = 0; i < totalMessages; i++)
        {
            var order = GenerateOrderMessage(userId, i);
            await publisher.PublishAsync(userId, order);
            Console.WriteLine($"[User {userId}] Published message {order.OrderId}");
            await Task.Delay(100);
        }
    }));
}

await Task.WhenAll(tasks);

publisher.Dispose();
