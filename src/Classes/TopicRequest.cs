namespace src.Classes;

public class ProduceTopicRequest(string topicName, List<ProducePartitionRequest> partitions)
{
  public string TopicName { get; } = topicName;
  public List<ProducePartitionRequest> Partitions { get; } = partitions;
}

public class ProducePartitionRequest(int partitionIndex, byte[]? records)
{
  public int PartitionIndex { get; } = partitionIndex;
  public byte[]? Records { get; } = records;
}

public class FetchTopicRequest(byte[] topicId, List<int> partitionIndexes)
{
  public byte[] TopicId { get; } = topicId;
  public List<int> PartitionIndexes { get; } = partitionIndexes;
}