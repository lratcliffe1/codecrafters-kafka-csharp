using src.Requests;
using src.Requests.Base;

namespace src;

public static class RequestParser
{
  public static IKafkaRequest ParseRequest(int messageSize, byte[] request)
  {
    var requestHeader = new RequestHeader(messageSize, request);

    return requestHeader.ApiKey switch
    {
      ApiKey.Produce => ProduceRequest.FromBytes(requestHeader, request),
      ApiKey.Fetch => FetchRequest.FromBytes(requestHeader, request),
      ApiKey.ApiVersions => ApiVersionsRequest.FromBytes(requestHeader, request),
      ApiKey.DescribeTopicPartitions => DescribeTopicPartitionsRequest.FromBytes(requestHeader, request),
      _ => UnsupportedRequest.FromBytes(requestHeader, request),
    };
  }
}
