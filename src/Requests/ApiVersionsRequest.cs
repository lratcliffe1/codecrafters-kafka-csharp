using System.Text;

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
    short errorCode = RequestHeader.ApiVersion > 4 ? (short)35 : (short)0;
    var apiVersionEntries = errorCode == 0 ? ApiKeyArray.Instance.Values.ToArray() : [];

    const int responseHeaderSize = 4; // correlation_id
    var apiKeysCount = apiVersionEntries.Length;

    // ApiVersionsResponse v4 body (flexible):
    // error_code (2)
    // api_keys COMPACT_ARRAY: length (uvarint) + N * (api_key(2) + min(2) + max(2) + tagged_fields(uvarint))
    // throttle_time_ms (4)
    // tagged_fields (uvarint)
    var responseBodySize =
      2 +
      1 +
      apiKeysCount * (2 + 2 + 2 + 1) +
      4 +
      1;

    var messageSize = responseHeaderSize + responseBodySize;
    var writer = new KafkaResponseWriter(messageSize, RequestHeader.CorrelationId);
    writer.WriteInt16(errorCode);
    writer.WriteCompactArrayLength(apiKeysCount);

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