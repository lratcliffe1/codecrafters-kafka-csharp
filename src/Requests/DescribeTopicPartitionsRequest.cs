using System.Text;

namespace src.Requests;

public class DescribeTopicPartitionsRequest(RequestHeader requestHeader, string topicName) : BaseKafkaRequest(requestHeader), IKafkaRequest
{
  public string TopicName { get; set; } = topicName;

  public static DescribeTopicPartitionsRequest FromBytes(RequestHeader requestHeader, byte[] request)
  {
    // Request header v2 includes a header TAG_BUFFER byte after client_id.
    // Body starts after: api_key(2) + api_version(2) + correlation_id(4) + client_id_len(2) + client_id + tagged_fields.
    var requestBodyOffset = 10 + requestHeader.ClientId.Length + 1;
    if (requestBodyOffset >= request.Length)
    {
      return new DescribeTopicPartitionsRequest(requestHeader, string.Empty);
    }

    var offset = requestBodyOffset;
    var topicsArrayLength = request[offset++];
    if (topicsArrayLength <= 1 || offset >= request.Length)
    {
      return new DescribeTopicPartitionsRequest(requestHeader, string.Empty);
    }

    // Parse only the first topic name from COMPACT_ARRAY<COMPACT_STRING>.
    var topicNameLength = request[offset++] - 1;
    if (topicNameLength <= 0 || offset + topicNameLength > request.Length)
    {
      return new DescribeTopicPartitionsRequest(requestHeader, string.Empty);
    }

    var topicName = Encoding.UTF8.GetString(request.AsSpan(offset, topicNameLength));
    return new DescribeTopicPartitionsRequest(requestHeader, topicName);
  }

  public override byte[] BuildResponse()
  {
    const int responseHeaderSize = 5; // correlation_id + TAG_BUFFER (header v1)
    var responseBodySize =
      4 + // throttle_time_ms
      1 + // topics compact array length
      2 + // topic error_code
      1 + // topic name compact length
      Encoding.UTF8.GetByteCount(TopicName) +
      16 + // topic_id (UUID all zeros)
      1 + // is_internal
      1 + // partitions compact array length (empty)
      4 + // topic_authorized_operations
      1 + // topic TAG_BUFFER
      1 + // next_cursor (nullable struct, null)
      1;  // response TAG_BUFFER

    var messageSize = responseHeaderSize + responseBodySize;
    var writer = new KafkaResponseWriter(messageSize, RequestHeader.CorrelationId);
    writer.WriteTagBufferEmpty(); // response header TAG_BUFFER
    writer.WriteInt32(0); // throttle_time_ms
    writer.WriteCompactArrayLength(1); // topics compact array with 1 element
    writer.WriteInt16(3); // UNKNOWN_TOPIC_OR_PARTITION
    writer.WriteCompactString(TopicName);

    // topic_id UUID = 00000000-0000-0000-0000-000000000000 (16 zero bytes)
    writer.Advance(16);
    writer.WriteByte(0); // is_internal = false
    writer.WriteCompactArrayLength(0); // partitions compact array with 0 elements
    writer.WriteInt32(0); // topic_authorized_operations
    writer.WriteTagBufferEmpty(); // topic TAG_BUFFER
    writer.WriteByte(0xff); // next_cursor = null
    writer.WriteTagBufferEmpty(); // response TAG_BUFFER

    return writer.ToArray();
  }
}