using src.Requests.Base;

namespace src.Requests;

public class DescribeTopicPartitionsRequest(RequestHeader requestHeader, List<string> topicNames) : BaseKafkaRequest(requestHeader), IKafkaRequest
{
  public List<string> TopicNames { get; set; } = topicNames;

  public static DescribeTopicPartitionsRequest FromBytes(RequestHeader requestHeader, byte[] request)
  {
    var offset = SharedRequestParsers.ReadRequestBodyOffset(requestHeader);
    var topicsCount = SharedRequestParsers.ReadCompactArrayCount(request, ref offset);
    var topicNames = new List<string>(topicsCount);

    for (var i = 0; i < topicsCount; i++)
    {
      var topicName = SharedRequestParsers.ReadCompactString(request, ref offset);
      topicNames.Add(topicName);
      SharedRequestParsers.SkipTagBuffer(request, ref offset);
    }

    return new DescribeTopicPartitionsRequest(requestHeader, topicNames);
  }

  public override byte[] BuildResponse()
  {
    var sortedTopicNames = TopicNames.OrderBy(topicName => topicName, StringComparer.Ordinal).ToList();

    var writer = new KafkaResponseWriter(RequestHeader.CorrelationId);
    writer.WriteTagBufferEmpty(); // response header TAG_BUFFER
    writer.WriteInt32(0); // throttle_time_ms
    writer.WriteCompactArrayLength(sortedTopicNames.Count); // topics

    foreach (var topicName in sortedTopicNames)
    {
      var topicMetadata = ClusterMetadata.GetTopicMetadataByName(topicName);
      writer.WriteInt16(topicMetadata != null ? (short)0 : (short)3); // UNKNOWN_TOPIC_OR_PARTITION when missing
      writer.WriteCompactString(topicName);

      if (topicMetadata != null)
      {
        writer.WriteBytes(topicMetadata.TopicId);
      }
      else
      {
        writer.Advance(16); // topic_id = all-zero UUID placeholder
      }

      writer.WriteByte(0); // is_internal = false
      writer.WriteCompactArrayLength(topicMetadata?.Partitions.Count ?? 0); // partitions

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
}