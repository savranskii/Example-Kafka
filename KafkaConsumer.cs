using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace WebAPI;

public class KafkaConsumer : BackgroundService
{
    private readonly ILogger<KafkaConsumer> _log;
    private readonly IConsumer<string, string> _consumer;
    private readonly KafkaConfigOptions _options;

    public KafkaConsumer(ILogger<KafkaConsumer> log, IOptionsMonitor<KafkaConfigOptions> options)
    {
        _log = log;
        _options = options.CurrentValue;

        var config = new ConsumerConfig
        {
            BootstrapServers = _options.Bootstrap,
            GroupId = _options.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();
        _consumer.Subscribe(_options.Topic);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        while (!stoppingToken.IsCancellationRequested)
        {
            var consumeResult = _consumer.Consume(stoppingToken);

            _log.LogInformation(consumeResult.Message.Key + " - " + consumeResult.Message.Value);
            _consumer.Commit();
        }
    }

    public override void Dispose()
    {
        _consumer.Dispose();
        base.Dispose();
    }
}
