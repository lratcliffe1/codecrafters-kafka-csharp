namespace src.Requests;

public class UnsupportedRequest(RequestHeader requestHeader) : BaseKafkaRequest(requestHeader), IKafkaRequest
{
  public override byte[] BuildResponse()
  {
    // Minimal response with just header and error code for unexpected paths.
    const int responseBodySize = 2;
    const int responseHeaderSize = 4;
    var messageSize = responseHeaderSize + responseBodySize;
    var writer = new KafkaResponseWriter(messageSize, RequestHeader.CorrelationId);
    writer.WriteInt16(35);

    return writer.ToArray();
  }
}