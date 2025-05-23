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
var now = DateTimeOffset.UtcNow;
var formatted = now.ToString("HH:mm:ss.fff");
Console.WriteLine($"{formatted}");


var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
var logger = loggerFactory.CreateLogger<KafkaMessageListener<OrderMessage>>();

var listener = new KafkaMessageListener<OrderMessage>(config,
    new JsonDeserializer<OrderMessage>(),
    logger,
    "kafka-topic-test"
);

long processed = 0;
long orders = 0;
var firstTimestamp = DateTimeOffset.UtcNow;

var strategy = Environment.GetEnvironmentVariable("PROCESSING_STRATEGY")?.ToUpperInvariant();

listener.Subscribe(async message =>
{
    await Task.Delay(10);

    Interlocked.Increment(ref processed);
    
    if (message.OrderId is "999" or "9999" or "99999")
    {
        Interlocked.Increment(ref orders);
    }
    
    if (orders != 0 && orders % 4 == 0 || (orders == 1 && strategy == "BATCH"))
    {
        Interlocked.Exchange(ref orders, 0);
        var endTimestamp = DateTimeOffset.UtcNow;
        Console.WriteLine("--- DONE ---");
        Console.WriteLine($"Strategy: {strategy ?? "SEQUENTIAL"}");
        Console.WriteLine($"Start: {firstTimestamp:HH:mm:ss.fff}");
        Console.WriteLine($"End:   {endTimestamp:HH:mm:ss.fff}");
        Console.WriteLine($"Throughput: {processed / (endTimestamp - firstTimestamp).TotalSeconds:F2} msgs");
    }
});

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    listener.Dispose();
};

await Task.Delay(-1);