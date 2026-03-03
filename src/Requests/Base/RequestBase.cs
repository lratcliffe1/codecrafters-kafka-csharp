using System.Buffers.Binary;
using System.Text;

namespace src.Requests.Base;

public interface IKafkaRequest
{
  byte[] BuildResponse();
}

public class BaseKafkaRequest(RequestHeader requestHeader) : IKafkaRequest
{
  public RequestHeader RequestHeader { get; set; } = requestHeader;

  public virtual byte[] BuildResponse()
  {
    throw new NotImplementedException();
  }
}

public class RequestHeader(int messageSize, ApiKey apiKey, int apiVersion, int correlationId, string clientId)
{
  public int MessageSize { get; set; } = messageSize;
  public ApiKey ApiKey { get; set; } = apiKey;
  public int ApiVersion { get; set; } = apiVersion;
  public int CorrelationId { get; set; } = correlationId;
  public string ClientId { get; set; } = clientId;

  public RequestHeader(int messageSize, byte[] request) : this(
    messageSize,
    (ApiKey)BinaryPrimitives.ReadInt16BigEndian(request.AsSpan(0, 2)),
    BinaryPrimitives.ReadInt16BigEndian(request.AsSpan(2, 2)),
    BinaryPrimitives.ReadInt32BigEndian(request.AsSpan(4, 4)),
    Encoding.UTF8.GetString(request.AsSpan(10, BinaryPrimitives.ReadInt16BigEndian(request.AsSpan(8, 2)))))
  { }
}