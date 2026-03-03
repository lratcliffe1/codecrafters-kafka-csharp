namespace src;

public enum ApiKey
{
  Unknown = 0,
  ApiVersions = 18,
  DescribeTopicPartitions = 75,
}

public class ApiKeyArray
{
  public static Dictionary<ApiKey, ApiKeyEntry> Instance { get; } = new Dictionary<ApiKey, ApiKeyEntry>()
  {
    [ApiKey.ApiVersions] = new ApiKeyEntry(ApiKey.ApiVersions, 0, 4),
    [ApiKey.DescribeTopicPartitions] = new ApiKeyEntry(ApiKey.DescribeTopicPartitions, 0, 0),
  };
}

public class ApiKeyEntry(ApiKey apiKey, short minVersion, short maxVersion)
{
  public ApiKey ApiKey { get; set; } = apiKey;
  public short MinVersion { get; set; } = minVersion;
  public short MaxVersion { get; set; } = maxVersion;
}