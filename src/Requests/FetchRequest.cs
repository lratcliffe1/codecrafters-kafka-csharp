using src.Requests.Base;

namespace src.Requests;

public class FetchRequest(RequestHeader requestHeader) : BaseKafkaRequest(requestHeader), IKafkaRequest
{
  public static FetchRequest FromBytes(RequestHeader requestHeader, byte[] request)
  {
    return new FetchRequest(requestHeader);
  }

  public override byte[] BuildResponse()
  {
    var writer = new KafkaResponseWriter(RequestHeader.CorrelationId);
    writer.WriteInt16(35);

    return writer.ToArray();
  }
}