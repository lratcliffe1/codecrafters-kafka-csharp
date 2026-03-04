using src.Classes;
using src.Helpers;
using src.Requests.Base;

namespace src.Requests;

public class ProduceRequest(RequestHeader requestHeader, List<ProduceTopicRequest> topics) : BaseKafkaRequest(requestHeader)
{
  public List<ProduceTopicRequest> Topics { get; } = topics;

  public static ProduceRequest FromBytes(RequestHeader requestHeader, byte[] request)
  {
    var offset = SharedRequestParsers.ReadRequestBodyOffset(requestHeader);
    var transactionalIdLength = SharedRequestParsers.ReadUnsignedVarInt(request, ref offset);
    if (transactionalIdLength > 0)
    {
      offset += transactionalIdLength - 1;
    }

    offset += 2 + 4; // acks + timeout_ms
    var topicsCount = SharedRequestParsers.ReadCompactArrayCount(request, ref offset);
    var topics = new List<ProduceTopicRequest>(topicsCount);

    for (var i = 0; i < topicsCount; i++)
    {
      var topicName = SharedRequestParsers.ReadCompactString(request, ref offset);
      var partitionsCount = SharedRequestParsers.ReadCompactArrayCount(request, ref offset);
      var partitions = new List<int>(partitionsCount);

      for (var j = 0; j < partitionsCount; j++)
      {
        var partitionIndex = SharedRequestParsers.ReadInt32(request, ref offset);
        partitions.Add(partitionIndex);

        var recordsLength = SharedRequestParsers.ReadUnsignedVarInt(request, ref offset);
        if (recordsLength > 0)
        {
          offset += recordsLength - 1;
        }

        SharedRequestParsers.SkipTagBuffer(request, ref offset);
      }

      SharedRequestParsers.SkipTagBuffer(request, ref offset);
      topics.Add(new ProduceTopicRequest(topicName, partitions));
    }

    SharedRequestParsers.SkipTagBuffer(request, ref offset);
    return new ProduceRequest(requestHeader, topics);
  }

  public override byte[] BuildResponse()
  {
    var writer = new KafkaResponseWriter(RequestHeader.CorrelationId);
    writer.WriteTagBufferEmpty(); // response header TAG_BUFFER
    writer.WriteCompactArrayLength(Topics.Count); // responses

    foreach (var topic in Topics)
    {
      writer.WriteCompactString(topic.TopicName);
      writer.WriteCompactArrayLength(topic.PartitionIndexes.Count);
      var topicMetadata = ClusterMetadata.GetTopicMetadataByName(topic.TopicName);

      foreach (var partitionIndex in topic.PartitionIndexes)
      {
        var partitionExists = topicMetadata?.Partitions.Any(p => p.PartitionId == partitionIndex) ?? false;
        var errorCode = partitionExists ? (short)0 : (short)3; // UNKNOWN_TOPIC_OR_PARTITION

        writer.WriteInt32(partitionIndex); // index
        writer.WriteInt16(errorCode);
        writer.WriteInt64(errorCode == 0 ? 0 : -1); // base_offset
        writer.WriteInt64(-1); // log_append_time_ms
        writer.WriteInt64(errorCode == 0 ? 0 : -1); // log_start_offset
        writer.WriteCompactArrayLength(0); // record_errors
        writer.WriteCompactNullableString(null); // error_message
        writer.WriteTagBufferEmpty(); // partition TAG_BUFFER
      }

      writer.WriteTagBufferEmpty(); // topic TAG_BUFFER
    }

    writer.WriteInt32(0); // throttle_time_ms
    writer.WriteTagBufferEmpty(); // response TAG_BUFFER
    return writer.ToArray();
  }
}
