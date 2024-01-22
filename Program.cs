using Confluent.Kafka;
using System.Diagnostics;
using System.Net;
using WebAPI;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

const string Topic = "customers";
const string Bootstrap = "localhost:9092";

var config = new ConsumerConfig
{
    BootstrapServers = Bootstrap,
    GroupId = "test_group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe(Topic);

builder.Services.AddHostedService(sp => new KafkaConsumer(sp.GetRequiredService<ILogger<KafkaConsumer>>(), consumer));
builder.Services.AddHostedService<KafkaConsumer>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapGet("/send-message", async (string text) =>
{
    ProducerConfig config = new()
    {
        BootstrapServers = Bootstrap,
    };

    try
    {
        using (var producer = new ProducerBuilder<string, string>(config).Build())
        {
            var result = await producer.ProduceAsync(Topic, new Message<string, string>
            {
                Value = text
            });

            Debug.WriteLine($"Delivery Timestamp:{ result.Timestamp.UtcDateTime}");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error occured: {ex.Message}");
    }

    return Results.NoContent();
})
.WithName("GetWeatherForecast")
.WithOpenApi();

app.Run();
