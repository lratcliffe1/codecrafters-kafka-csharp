namespace src;

public class BaseKafkaRequest(RequestHeader requestHeader)
{
  public RequestHeader RequestHeader { get; set; } = requestHeader;
}

public class RequestHeader(int messageSize, int apiKey, int apiVersion, int correlationId, string clientId)
{
  public int MessageSize { get; set; } = messageSize;
  public int ApiKey { get; set; } = apiKey;
  public int ApiVersion { get; set; } = apiVersion;
  public int CorrelationId { get; set; } = correlationId;
  public string ClientId { get; set; } = clientId;
}

public class ApiVersionsRequest(RequestHeader requestHeader, string clientSoftwareVersion) : BaseKafkaRequest(requestHeader)
{
  public string ClientId { get; set; } = requestHeader.ClientId;
  public string ClientSoftwareVersion { get; set; } = clientSoftwareVersion;
}