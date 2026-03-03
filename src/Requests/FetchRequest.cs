using src.Requests.Base;

namespace src.Requests;

public class FetchRequest(RequestHeader requestHeader, List<string> topicNames) : BaseKafkaRequest(requestHeader), IKafkaRequest
{
  public List<string> TopicNames { get; set; } = topicNames;

  public static FetchRequest FromBytes(RequestHeader requestHeader, byte[] request)
  {
    return new FetchRequest(requestHeader, SharedRequestParsers.ParseTopicNamesFromCompactArray(requestHeader, request));
  }

  // Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] [node_endpoints]<tag: 0> 
  //   throttle_time_ms => INT32
  //   error_code => INT16
  //   session_id => INT32
  //   responses => topic_id [partitions] 
  //     topic_id => UUID
  //     partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records diverging_epoch<tag: 0> current_leader<tag: 1> snapshot_id<tag: 2> 
  //       partition_index => INT32
  //       error_code => INT16
  //       high_watermark => INT64
  //       last_stable_offset => INT64
  //       log_start_offset => INT64
  //       aborted_transactions => producer_id first_offset 
  //         producer_id => INT64
  //         first_offset => INT64
  //       preferred_read_replica => INT32
  //       records => COMPACT_RECORDS
  //       diverging_epoch<tag: 0> => epoch end_offset 
  //         epoch => INT32
  //         end_offset => INT64
  //       current_leader<tag: 1> => leader_id leader_epoch 
  //         leader_id => INT32
  //         leader_epoch => INT32
  //       snapshot_id<tag: 2> => end_offset epoch 
  //         end_offset => INT64
  //         epoch => INT32
  //   node_endpoints<tag: 0> => node_id host port rack 
  //     node_id => INT32
  //     host => COMPACT_STRING
  //     port => INT32
  //     rack => COMPACT_NULLABLE_STRING
  public override byte[] BuildResponse()
  {
    var sortedTopicNames = TopicNames.OrderBy(topicName => topicName, StringComparer.Ordinal).ToList();

    var writer = new KafkaResponseWriter(RequestHeader.CorrelationId);
    writer.WriteTagBufferEmpty(); // response header TAG_BUFFER
    writer.WriteInt32(0); // throttle_time_ms
    writer.WriteInt16(0); // error_code
    writer.WriteInt32(0); // session_id
    writer.WriteCompactArrayLength(sortedTopicNames.Count); // responses

    foreach (var topicName in sortedTopicNames)
    {
      var topicMetadata = ClusterMetadata.GetTopicMetadata(topicName);
      if (topicMetadata != null)
      {
        writer.WriteBytes(topicMetadata.TopicId);
      }
      else
      {
        writer.Advance(16);
      }

      writer.WriteCompactArrayLength(topicMetadata?.Partitions.Count ?? 0);

      foreach (var partition in topicMetadata?.Partitions ?? [])
      {
        writer.WriteInt32(partition.PartitionId);
        writer.WriteInt16(0); // partition error_code
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

    writer.WriteTagBufferEmpty(); // response TAG_BUFFER (node_endpoints absent)

    return writer.ToArray();
  }

}