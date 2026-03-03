using System.Buffers.Binary;

namespace src;

public static class ResponseBuilder
{
  public static byte[] BuildResponse(BaseKafkaRequest request)
  {
    return request.RequestHeader.ApiKey switch
    {
      18 => BuildApiVersionsResponse(request),
      _ => BuildUnsupportedResponse(request),
    };
  }

  static byte[] BuildApiVersionsResponse(BaseKafkaRequest request)
  {
    short errorCode = request.RequestHeader.ApiVersion > 4 ? (short)35 : (short)0;
    var includeApiVersionsKey = errorCode == 0;

    const int responseHeaderSize = 4; // correlation_id
    var apiKeysCount = includeApiVersionsKey ? 1 : 0;

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
    var response = new byte[4 + messageSize];

    BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(0, 4), messageSize);
    BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(4, 4), request.RequestHeader.CorrelationId);

    var offset = 8;

    BinaryPrimitives.WriteInt16BigEndian(response.AsSpan(offset, 2), errorCode);
    offset += 2;

    // Compact array length is element_count + 1.
    response[offset++] = (byte)(apiKeysCount + 1);

    if (includeApiVersionsKey)
    {
      BinaryPrimitives.WriteInt16BigEndian(response.AsSpan(offset, 2), 18); // ApiVersions
      offset += 2;

      BinaryPrimitives.WriteInt16BigEndian(response.AsSpan(offset, 2), 0); // min_version
      offset += 2;

      BinaryPrimitives.WriteInt16BigEndian(response.AsSpan(offset, 2), 4); // max_version
      offset += 2;

      response[offset++] = 0; // entry TAG_BUFFER (empty)
    }

    BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(offset, 4), 0); // throttle_time_ms
    offset += 4;

    response[offset++] = 0; // response TAG_BUFFER (empty)

    return response;
  }

  static byte[] BuildUnsupportedResponse(BaseKafkaRequest request)
  {
    // Minimal response with just header and error code for unexpected paths.
    const int responseBodySize = 2;
    const int responseHeaderSize = 4;
    var messageSize = responseHeaderSize + responseBodySize;
    var response = new byte[4 + messageSize];

    BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(0, 4), messageSize);
    BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(4, 4), request.RequestHeader.CorrelationId);
    BinaryPrimitives.WriteInt16BigEndian(response.AsSpan(8, 2), 35);

    return response;
  }
}
