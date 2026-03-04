using src.Requests.Base;

namespace src.Requests;

public class FetchRequest(RequestHeader requestHeader, List<FetchTopicRequest> topics) : BaseKafkaRequest(requestHeader)
{
  public List<FetchTopicRequest> Topics { get; set; } = topics;

  public static FetchRequest FromBytes(RequestHeader requestHeader, byte[] request)
  {
    var offset = SharedRequestParsers.ReadRequestBodyOffset(requestHeader);
    offset += 4 + 4 + 4 + 1 + 4 + 4; // replica_id..session_epoch
    var topicsCount = SharedRequestParsers.ReadCompactArrayCount(request, ref offset);
    var topics = new List<FetchTopicRequest>(topicsCount);

    for (var i = 0; i < topicsCount; i++)
    {
      var topicId = SharedRequestParsers.ReadUuid(request, ref offset);
      var partitionsCount = SharedRequestParsers.ReadCompactArrayCount(request, ref offset);
      var partitionIndexes = new List<int>(partitionsCount);

      for (var j = 0; j < partitionsCount; j++)
      {
        var partitionIndex = SharedRequestParsers.ReadInt32(request, ref offset);
        offset += 4 + 8 + 4 + 8 + 4; // current_leader_epoch..partition_max_bytes
        partitionIndexes.Add(partitionIndex);
        SharedRequestParsers.SkipTagBuffer(request, ref offset);
      }

      SharedRequestParsers.SkipTagBuffer(request, ref offset); // topic TAG_BUFFER
      topics.Add(new FetchTopicRequest(topicId, partitionIndexes));
    }

    return new FetchRequest(requestHeader, topics);
  }

  public override byte[] BuildResponse()
  {
    var writer = new KafkaResponseWriter(RequestHeader.CorrelationId);
    writer.WriteTagBufferEmpty(); // response header TAG_BUFFER
    writer.WriteInt32(0); // throttle_time_ms
    writer.WriteInt16(0); // error_code
    writer.WriteInt32(0); // session_id
    writer.WriteCompactArrayLength(Topics.Count); // responses

    foreach (var topic in Topics)
    {
      writer.WriteBytes(topic.TopicId);

      var topicMetadata = ClusterMetadata.GetTopicMetadataById(topic.TopicId);
      writer.WriteCompactArrayLength(topic.PartitionIndexes.Count);
      foreach (var partitionIndex in topic.PartitionIndexes)
      {
        writer.WriteInt32(partitionIndex);
        writer.WriteInt16(topicMetadata != null ? (short)0 : (short)100); // UNKNOWN_TOPIC_ID
        writer.WriteInt64(0); // high_watermark
        writer.WriteInt64(0); // last_stable_offset
        writer.WriteInt64(0); // log_start_offset
        writer.WriteCompactArrayLength(0); // aborted_transactions
        writer.WriteInt32(-1); // preferred_read_replica
        writer.WriteByte(0); // records = null COMPACT_RECORDS
        writer.WriteTagBufferEmpty(); // partition TAG_BUFFER
      }

      writer.WriteTagBufferEmpty(); // topic TAG_BUFFER
    }

    writer.WriteTagBufferEmpty(); // response TAG_BUFFER (node_endpoints tagged field omitted)

    return writer.ToArray();
  }
}

public class FetchTopicRequest(byte[] topicId, List<int> partitionIndexes)
{
  public byte[] TopicId { get; } = topicId;
  public List<int> PartitionIndexes { get; } = partitionIndexes;
}