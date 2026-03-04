namespace src;

public enum ApiKey
{
  Produce = 0,
  Fetch = 1,
  ApiVersions = 18,
  DescribeTopicPartitions = 75,
}

public class ApiKeyArray
{
  public static Dictionary<ApiKey, ApiKeyEntry> Instance { get; } = new Dictionary<ApiKey, ApiKeyEntry>()
  {
    [ApiKey.Produce] = new ApiKeyEntry(ApiKey.Produce, 0, 11),
    [ApiKey.Fetch] = new ApiKeyEntry(ApiKey.Fetch, 0, 16),
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