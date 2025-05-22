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
var logger = loggerFactory.CreateLogger<KafkaMessagePublisher<OrderMessage>>();

var publisher = new KafkaMessagePublisher<OrderMessage>(
    config,
    topic: "kafka-topic-test",
    serializer: new JsonSerializer<OrderMessage>(),
    logger
);

OrderMessage GenerateOrderMessage(string userId)
{
    return new OrderMessage(
        OrderId: Guid.NewGuid().ToString(),
        UserId: userId,
        ProductId: $"product_{Random.Shared.Next(1, 100)}",
        Quantity: Random.Shared.Next(1, 10),
        Price: Math.Round((decimal)(Random.Shared.NextDouble() * 100), 2),
        Timestamp: DateTime.UtcNow
    );
}

var totalMessages = int.TryParse(Environment.GetEnvironmentVariable("TOTAL_MESSAGES"), out var n) ? n : 1000;
for (var i = 0; i < totalMessages; i++)
{
    var userId = $"user_{Random.Shared.Next(1, 10)}";
    var order = GenerateOrderMessage(userId);
    await publisher.PublishAsync(userId, order);
    Console.WriteLine($"Published message {order.OrderId} for User {userId}");
    await Task.Delay(100);
}

publisher.Dispose();
