using src.Requests;

namespace src;

public static class RequestParser
{
  public static BaseKafkaRequest ParseRequest(int messageSize, byte[] request)
  {
    var requestHeader = new RequestHeader(messageSize, request);

    return requestHeader.ApiKey switch
    {
      ApiKey.ApiVersions => ApiVersionsRequest.FromBytes(requestHeader, request),
      ApiKey.DescribeTopicPartitions => DescribeTopicPartitionsRequest.FromBytes(requestHeader, request),
      _ => new UnsupportedRequest(requestHeader),
    };
  }
}
