namespace src;

public class BaseKafkaRequest(RequestHeader requestHeader)
{
  public RequestHeader RequestHeader { get; set; } = requestHeader;
}

public class RequestHeader(int messageSize, ApiKey apiKey, int apiVersion, int correlationId, string clientId)
{
  public int MessageSize { get; set; } = messageSize;
  public ApiKey ApiKey { get; set; } = apiKey;
  public int ApiVersion { get; set; } = apiVersion;
  public int CorrelationId { get; set; } = correlationId;
  public string ClientId { get; set; } = clientId;
}

public class ApiVersionsRequest(RequestHeader requestHeader, string clientSoftwareVersion) : BaseKafkaRequest(requestHeader)
{
  public string ClientId { get; set; } = requestHeader.ClientId;
  public string ClientSoftwareVersion { get; set; } = clientSoftwareVersion;
}

public enum ApiKey
{
  Unknown = 0,
  ApiVersions = 18,
  DescribeTopicPartitions = 75,
}