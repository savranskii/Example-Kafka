namespace WebAPI;

public class KafkaConfigOptions
{
    public const string KafkaConfig = "KafkaConfig";

    public string Topic { get; set; } = string.Empty;
    public string Bootstrap { get; set; } = string.Empty;
    public string GroupId { get; set; } = string.Empty;
}
