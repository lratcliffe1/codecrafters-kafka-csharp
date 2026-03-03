using System.Text;
using src.Requests.Base;

namespace src.Requests;

public class DescribeTopicPartitionsRequest(RequestHeader requestHeader, List<string> topicNames) : BaseKafkaRequest(requestHeader), IKafkaRequest
{
  public List<string> TopicNames { get; set; } = topicNames;

  public static DescribeTopicPartitionsRequest FromBytes(RequestHeader requestHeader, byte[] request)
  {
    // Request header v2 includes a header TAG_BUFFER byte after client_id.
    // Body starts after: api_key(2) + api_version(2) + correlation_id(4) + client_id_len(2) + client_id + tagged_fields.
    var requestBodyOffset = 10 + requestHeader.ClientId.Length + 1;
    if (requestBodyOffset >= request.Length)
    {
      return new DescribeTopicPartitionsRequest(requestHeader, []);
    }

    var offset = requestBodyOffset;
    if (!TryReadUnsignedVarInt(request, ref offset, out var encodedTopicsLength))
    {
      return new DescribeTopicPartitionsRequest(requestHeader, []);
    }

    var topicsCount = encodedTopicsLength - 1;
    if (topicsCount <= 0)
    {
      return new DescribeTopicPartitionsRequest(requestHeader, []);
    }

    // Parse topics from COMPACT_ARRAY<topic_name + TAG_BUFFER>.
    var topicNames = new List<string>(topicsCount);
    for (var i = 0; i < topicsCount; i++)
    {
      if (!TryReadCompactString(request, ref offset, out var topicName))
      {
        break;
      }

      topicNames.Add(topicName);

      if (!TrySkipTagBuffer(request, ref offset))
      {
        break;
      }
    }

    return new DescribeTopicPartitionsRequest(requestHeader, topicNames);
  }

  public override byte[] BuildResponse()
  {
    var sortedTopicNames = TopicNames.OrderBy(topicName => topicName, StringComparer.Ordinal).ToList();

    var writer = new KafkaResponseWriter(RequestHeader.CorrelationId);
    writer.WriteTagBufferEmpty(); // response header TAG_BUFFER
    writer.WriteInt32(0); // throttle_time_ms
    writer.WriteCompactArrayLength(sortedTopicNames.Count);

    foreach (var topicName in sortedTopicNames)
    {
      var topicMetadata = ClusterMetadata.GetTopicMetadata(topicName);
      writer.WriteInt16(topicMetadata != null ? (short)0 : (short)3);
      writer.WriteCompactString(topicName);

      if (topicMetadata != null)
      {
        writer.WriteBytes(topicMetadata.TopicId);
      }
      else
      {
        writer.Advance(16);
      }

      writer.WriteByte(0); // is_internal = false
      writer.WriteCompactArrayLength(topicMetadata?.Partitions.Count ?? 0);

      if (topicMetadata != null)
      {
        foreach (var partition in topicMetadata.Partitions)
        {
          writer.WriteInt16(0); // partition error_code
          writer.WriteInt32(partition.PartitionId);
          writer.WriteInt32(partition.LeaderId);
          writer.WriteInt32(partition.LeaderEpoch);

          writer.WriteCompactArrayLength(partition.Replicas.Count);
          foreach (var brokerId in partition.Replicas)
          {
            writer.WriteInt32(brokerId);
          }

          writer.WriteCompactArrayLength(partition.Isr.Count);
          foreach (var brokerId in partition.Isr)
          {
            writer.WriteInt32(brokerId);
          }

          writer.WriteCompactArrayLength(partition.EligibleLeaderReplicas.Count);
          foreach (var brokerId in partition.EligibleLeaderReplicas)
          {
            writer.WriteInt32(brokerId);
          }

          writer.WriteCompactArrayLength(partition.LastKnownElr.Count);
          foreach (var brokerId in partition.LastKnownElr)
          {
            writer.WriteInt32(brokerId);
          }

          writer.WriteCompactArrayLength(0); // offline_replicas
          writer.WriteTagBufferEmpty(); // partition TAG_BUFFER
        }
      }

      writer.WriteInt32(0); // topic_authorized_operations
      writer.WriteTagBufferEmpty(); // topic TAG_BUFFER
    }

    writer.WriteByte(0xff); // next_cursor = null
    writer.WriteTagBufferEmpty(); // response TAG_BUFFER

    return writer.ToArray();
  }

  static bool TryReadCompactString(byte[] data, ref int offset, out string value)
  {
    value = string.Empty;
    if (!TryReadUnsignedVarInt(data, ref offset, out var encodedLength) || encodedLength <= 0)
    {
      return false;
    }

    var length = encodedLength - 1;
    if (offset + length > data.Length)
    {
      return false;
    }

    value = Encoding.UTF8.GetString(data.AsSpan(offset, length));
    offset += length;
    return true;
  }

  static bool TrySkipTagBuffer(byte[] data, ref int offset)
  {
    if (!TryReadUnsignedVarInt(data, ref offset, out var tagCount))
    {
      return false;
    }

    for (var i = 0; i < tagCount; i++)
    {
      if (!TryReadUnsignedVarInt(data, ref offset, out _) ||
          !TryReadUnsignedVarInt(data, ref offset, out var tagSize) ||
          tagSize < 0 ||
          offset + tagSize > data.Length)
      {
        return false;
      }

      offset += tagSize;
    }

    return true;
  }

  static bool TryReadUnsignedVarInt(byte[] data, ref int offset, out int value)
  {
    value = 0;
    var shift = 0;
    while (offset < data.Length && shift <= 28)
    {
      var current = data[offset++];
      value |= (current & 0x7F) << shift;
      if ((current & 0x80) == 0)
      {
        return true;
      }

      shift += 7;
    }

    return false;
  }
}