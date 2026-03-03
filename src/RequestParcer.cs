using System.Buffers.Binary;
using System.Text;

namespace src;

public static class RequestParcer
{
  public static BaseKafkaRequest? ParseRequest(int messageSize, byte[] request)
  {
    var requestHeader = ParseRequestHeader(messageSize, request);

    return requestHeader.ApiKey switch
    {
      ApiKey.ApiVersions => ParseApiVersionsRequest(requestHeader, request),
      _ => null,
    };
  }

  static RequestHeader ParseRequestHeader(int messageSize, byte[] request)
  {
    var apiKey = (ApiKey)BinaryPrimitives.ReadInt16BigEndian(request.AsSpan(0, 2));
    var apiVersion = BinaryPrimitives.ReadInt16BigEndian(request.AsSpan(2, 2));
    var correlationId = BinaryPrimitives.ReadInt32BigEndian(request.AsSpan(4, 4));
    var clientIdSize = BinaryPrimitives.ReadInt16BigEndian(request.AsSpan(8, 2));
    var clientId = Encoding.UTF8.GetString(request.AsSpan(10, clientIdSize));

    return new RequestHeader(messageSize, apiKey, apiVersion, correlationId, clientId);
  }

  static ApiVersionsRequest? ParseApiVersionsRequest(RequestHeader requestHeader, byte[] request)
  {
    var clientSoftwareVersion = Encoding.UTF8.GetString(request.AsSpan(10 + requestHeader.ClientId.Length, request.Length - 10 - requestHeader.ClientId.Length));
    return new ApiVersionsRequest(requestHeader, clientSoftwareVersion);
  }

}