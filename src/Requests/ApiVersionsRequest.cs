using System.Text;
using src.Requests.Base;

namespace src.Requests;

public class ApiVersionsRequest(RequestHeader requestHeader, string clientSoftwareVersion) : BaseKafkaRequest(requestHeader), IKafkaRequest
{
  public string ClientId { get; set; } = requestHeader.ClientId;
  public string ClientSoftwareVersion { get; set; } = clientSoftwareVersion;

  public static ApiVersionsRequest FromBytes(RequestHeader requestHeader, byte[] request)
  {
    var clientSoftwareVersion = Encoding.UTF8.GetString(request.AsSpan(10 + requestHeader.ClientId.Length, request.Length - 10 - requestHeader.ClientId.Length));
    return new ApiVersionsRequest(requestHeader, clientSoftwareVersion);
  }

  public override byte[] BuildResponse()
  {
    var errorCode = RequestHeader.ApiVersion > 4 ? (short)35 : (short)0;
    var apiVersionEntries = errorCode == 0 ? ApiKeyArray.Instance.Values.ToArray() : [];

    var writer = new KafkaResponseWriter(RequestHeader.CorrelationId);
    writer.WriteInt16(errorCode);
    writer.WriteCompactArrayLength(apiVersionEntries.Length);

    foreach (var apiVersionEntry in apiVersionEntries)
    {
      writer.WriteInt16((short)apiVersionEntry.ApiKey);
      writer.WriteInt16(apiVersionEntry.MinVersion);
      writer.WriteInt16(apiVersionEntry.MaxVersion);
      writer.WriteTagBufferEmpty();
    }

    writer.WriteInt32(0); // throttle_time_ms
    writer.WriteTagBufferEmpty();

    return writer.ToArray();
  }
}