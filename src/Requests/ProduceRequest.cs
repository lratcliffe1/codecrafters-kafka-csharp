using src.Requests.Base;

namespace src.Requests;

public class ProduceRequest(RequestHeader requestHeader) : BaseKafkaRequest(requestHeader)
{
  public static ProduceRequest FromBytes(RequestHeader requestHeader, byte[] request)
  {
    throw new NotImplementedException();
  }

  public override byte[] BuildResponse()
  {
    throw new NotImplementedException();
  }
}