using Confluent.Kafka;

namespace WebAPI;

public class KafkaConsumer : BackgroundService
{
    private readonly ILogger<KafkaConsumer> _log;
    private readonly IConsumer<string, string> _consumer;

    public KafkaConsumer(ILogger<KafkaConsumer> log, IConsumer<string, string> consumer)
    {
        _log = log;
        _consumer = consumer;
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
