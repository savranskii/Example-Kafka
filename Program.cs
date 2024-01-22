using Confluent.Kafka;
using Microsoft.Extensions.Options;
using WebAPI;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.Configure<KafkaConfigOptions>(builder.Configuration.GetSection(KafkaConfigOptions.KafkaConfig));
builder.Services.AddHostedService<KafkaConsumer>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapGet("/send-message", async (string text, IOptionsMonitor<KafkaConfigOptions> options) =>
{
    ProducerConfig config = new()
    {
        BootstrapServers = options.CurrentValue.Bootstrap,
    };

    try
    {
        using var producer = new ProducerBuilder<string, string>(config).Build();
        var result = await producer.ProduceAsync(options.CurrentValue.Topic, new Message<string, string>
        {
            Value = text
        });

        Console.WriteLine($"Delivery Timestamp:{result.Timestamp.UtcDateTime}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error occurred: {ex.Message}");
    }

    return Results.NoContent();
})
.WithName("SendMessage")
.WithOpenApi();

app.Run();
