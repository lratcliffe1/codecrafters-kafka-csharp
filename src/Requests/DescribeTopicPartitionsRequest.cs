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
    var topicMetadata = ClusterMetadata.GetTopicMetadata(TopicName);
    var topicExists = topicMetadata != null;
    var topicErrorCode = topicExists ? (short)0 : (short)3; // UNKNOWN_TOPIC_OR_PARTITION when missing

    const int responseHeaderSize = 5; // correlation_id + TAG_BUFFER (header v1)
    var topicNameBytesCount = Encoding.UTF8.GetByteCount(TopicName);
    var partitionCount = topicMetadata?.Partitions.Count ?? 0;

    var partitionsSize = 1; // compact array length
    if (topicMetadata != null)
    {
      foreach (var partition in topicMetadata.Partitions)
      {
        partitionsSize +=
          2 + // partition error_code
          4 + // partition_index
          4 + // leader_id
          4 + // leader_epoch
          1 + (partition.Replicas.Count * 4) + // replica_nodes compact array
          1 + (partition.Isr.Count * 4) + // isr_nodes compact array
          1 + (partition.EligibleLeaderReplicas.Count * 4) + // eligible_leader_replicas compact array
          1 + (partition.LastKnownElr.Count * 4) + // last_known_elr compact array
          1 + // offline_replicas compact array (empty)
          1; // partition TAG_BUFFER
      }
    }

    var responseBodySize =
      4 + // throttle_time_ms
      1 + // topics compact array length
      2 + // topic error_code
      1 + topicNameBytesCount + // topic name compact string
      16 + // topic_id
      1 + // is_internal
      partitionsSize +
      4 + // topic_authorized_operations
      1 + // topic TAG_BUFFER
      1 + // next_cursor (nullable struct, null)
      1;  // response TAG_BUFFER

    var messageSize = responseHeaderSize + responseBodySize;
    var writer = new KafkaResponseWriter(messageSize, RequestHeader.CorrelationId);
    writer.WriteTagBufferEmpty(); // response header TAG_BUFFER
    writer.WriteInt32(0); // throttle_time_ms
    writer.WriteCompactArrayLength(1); // topics compact array with 1 element
    writer.WriteInt16(topicErrorCode);
    writer.WriteCompactString(TopicName);

    if (topicMetadata != null)
    {
      writer.WriteBytes(topicMetadata.TopicId);
    }
    else
    {
      writer.Advance(16);
    }
    writer.WriteByte(0); // is_internal = false
    writer.WriteCompactArrayLength(partitionCount);

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
    writer.WriteByte(0xff); // next_cursor = null
    writer.WriteTagBufferEmpty(); // response TAG_BUFFER

    return writer.ToArray();
  }
}