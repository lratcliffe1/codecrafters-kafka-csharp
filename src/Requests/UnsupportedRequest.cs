using src.Helpers;
using src.Requests.Base;

namespace src.Requests;

public class UnsupportedRequest(RequestHeader requestHeader) : BaseKafkaRequest(requestHeader)
{
  public static UnsupportedRequest FromBytes(RequestHeader requestHeader, byte[] request)
  {
    return new UnsupportedRequest(requestHeader);
  }

  public override byte[] BuildResponse()
  {
    var writer = new KafkaResponseWriter(RequestHeader.CorrelationId);
    writer.WriteInt16(35);

    return writer.ToArray();
  }
}