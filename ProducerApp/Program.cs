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
var users = new[]
{
    "userkey_13", // i=0 → partition 0
    "userkey_22", // i=1 → partition 1
    "userkey_5",  // i=2 → partition 2
    "userkey_15", // i=3 → partition 3
    "userkey_10", // i=4 → partition 0
    "userkey_18", // i=5 → partition 1
    "userkey_4",  // i=6 → partition 2
    "userkey_12", // i=7 → partition 3
    "userkey_9",  // i=8 → partition 0
    "userkey_17", // i=9 → partition 1
    "userkey_3",  // i=10 → partition 2
    "userkey_11", // i=11 → partition 3
    "userkey_6",  // i=12 → partition 0
    "userkey_1",  // i=13 → partition 1
    "userkey_2",  // i=14 → partition 2
    "userkey_0"   // i=15 → partition 3
};


for (var userIndex = 0; userIndex < 16; userIndex++)
{
    var userId = users[userIndex];
    tasks.Add(Task.Run(async () =>
    {
        for (int i = 0; i < totalMessages; i++)
        {
            var order = GenerateOrderMessage(userId, i);
            await publisher.PublishAsync(userId, order);

            if (i % 100 == 0)
            {
                Console.WriteLine($"Published message {i} messages");
            }
        }
    }));
}

await Task.WhenAll(tasks);

publisher.Dispose();
