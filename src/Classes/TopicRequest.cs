namespace src.Classes;

public class ProduceTopicRequest(string topicName, List<int> partitionIndexes)
{
  public string TopicName { get; } = topicName;
  public List<int> PartitionIndexes { get; } = partitionIndexes;
}

public class FetchTopicRequest(byte[] topicId, List<int> partitionIndexes)
{
  public byte[] TopicId { get; } = topicId;
  public List<int> PartitionIndexes { get; } = partitionIndexes;
}