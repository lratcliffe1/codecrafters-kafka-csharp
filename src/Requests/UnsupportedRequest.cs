using src.Requests.Base;

namespace src.Requests;

public class UnsupportedRequest(RequestHeader requestHeader) : BaseKafkaRequest(requestHeader)
{
  public override byte[] BuildResponse()
  {
    var writer = new KafkaResponseWriter(RequestHeader.CorrelationId);
    writer.WriteInt16(35);

    return writer.ToArray();
  }
}