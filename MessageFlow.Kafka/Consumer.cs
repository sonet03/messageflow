using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Confluent.Kafka;
using MessageFlow.Kafka.Strategies;
using Microsoft.Extensions.Logging;

namespace MessageFlow.Kafka
{
    public class Consumer : IDisposable
    {
        protected readonly IConsumer<string, string> _consumer;
        protected readonly string _topic;
        private readonly ILogger<Consumer> _logger;
        protected bool _isRunning;
        private readonly IProcessingStrategy _processingStrategy;

        protected Consumer(string bootstrapServers, string groupId, string topic, ILoggerFactory loggerFactory,
            IProcessingStrategy? processingStrategy = null, Dictionary<string, string>? config = null)
        {
            _topic = topic;
            _logger = loggerFactory.CreateLogger<Consumer>();
            _processingStrategy = processingStrategy ?? new SequentialProcessingStrategy();

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            if (config != null)
            {
                foreach (var item in config)
                {
                    consumerConfig.Set(item.Key, item.Value);
                }
            }

            _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
            _consumer.Subscribe(_topic);
        }

        public async Task StartConsumingAsync(Func<string, string, Task> messageHandler, CancellationToken cancellationToken = default)
        {
            await StartConsumingInternal(messageHandler, cancellationToken);
        }

        private async Task StartConsumingInternal(Func<string, string, Task> messageHandler, CancellationToken cancellationToken)
        {
            _isRunning = true;

            try
            {
                while (_isRunning && !cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _consumer.Consume(cancellationToken);

                        if (consumeResult?.Message?.Value == null)
                            continue;

                        await _processingStrategy.Execute(consumeResult);

                        _consumer.Commit(consumeResult);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex,"Error consuming message");
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
            finally
            {
                _consumer.Close();
            }
        }

        public void Stop()
        {
            _isRunning = false;
        }

        public void Dispose()
        {
            _consumer?.Dispose();
        }
    }
}