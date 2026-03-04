using System.Buffers.Binary;
using System.Text;

namespace src;

public static class ClusterMetadata
{
  const string DataLogsRootPath = "/tmp/kraft-combined-logs";
  const string ClusterMetadataLogPath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

  // MetadataRecordType api keys from Kafka source:
  // 2 = TopicRecord, 3 = PartitionRecord
  const int TopicRecordApiKey = 2;
  const int PartitionRecordApiKey = 3;

  static readonly Dictionary<string, TopicMetadata> TopicsByName = new(StringComparer.Ordinal);
  static readonly Dictionary<string, TopicMetadata> TopicsById = new(StringComparer.Ordinal);

  public static void LoadMetadata()
  {
    if (!File.Exists(ClusterMetadataLogPath))
    {
      return;
    }

    var bytes = File.ReadAllBytes(ClusterMetadataLogPath);
    ParseRecordBatches(bytes);
  }

  public static TopicMetadata? GetTopicMetadataByName(string topicName)
  {
    TopicsByName.TryGetValue(topicName, out var topic);
    return topic;
  }

  public static TopicMetadata? GetTopicMetadataById(byte[] topicId)
  {
    TopicsById.TryGetValue(Convert.ToHexString(topicId), out var topic);
    return topic;
  }

  public static byte[]? GetPartitionRecordBatch(byte[] topicId, int partitionId)
  {
    var topic = GetTopicMetadataById(topicId);
    if (topic == null || !topic.Partitions.Any(p => p.PartitionId == partitionId))
    {
      return null;
    }

    var partitionDirectory = Path.Combine(DataLogsRootPath, $"{topic.TopicName}-{partitionId}");
    if (!Directory.Exists(partitionDirectory))
    {
      return null;
    }

    var logPath = Directory.GetFiles(partitionDirectory, "*.log")
      .OrderBy(path => path, StringComparer.Ordinal)
      .LastOrDefault();

    if (string.IsNullOrEmpty(logPath))
    {
      return null;
    }

    return File.ReadAllBytes(logPath);
  }

  public static void AppendPartitionRecordBatch(string topicName, int partitionId, byte[] records)
  {
    var topic = GetTopicMetadataByName(topicName);
    if (topic == null)
    {
      return;
    }

    if (!topic.Partitions.Any(p => p.PartitionId == partitionId))
    {
      return;
    }

    var partitionDirectory = Path.Combine(DataLogsRootPath, $"{topic.TopicName}-{partitionId}");
    Directory.CreateDirectory(partitionDirectory);
    var logPath = Path.Combine(partitionDirectory, "00000000000000000000.log");
    using var stream = new FileStream(logPath, FileMode.Append, FileAccess.Write, FileShare.Read);
    stream.Write(records, 0, records.Length);
  }

  static void ParseRecordBatches(byte[] logBytes)
  {
    var offset = 0;
    while (offset + 12 <= logBytes.Length)
    {
      var batchLength = BinaryPrimitives.ReadInt32BigEndian(logBytes.AsSpan(offset + 8, 4));
      if (batchLength <= 0)
      {
        break;
      }

      var batchTotalSize = 12 + batchLength;
      if (offset + batchTotalSize > logBytes.Length)
      {
        break;
      }

      // Kafka record batch v2 fixed header size is 61 bytes from batch start.
      if (offset + 61 > logBytes.Length)
      {
        break;
      }

      var recordsCount = BinaryPrimitives.ReadInt32BigEndian(logBytes.AsSpan(offset + 57, 4));
      var recordsOffset = offset + 61;
      var batchEnd = offset + batchTotalSize;

      ParseBatchRecords(logBytes, recordsOffset, batchEnd, recordsCount);
      offset += batchTotalSize;
    }
  }

  static void ParseBatchRecords(byte[] data, int start, int end, int recordsCount)
  {
    var offset = start;

    for (var i = 0; i < recordsCount && offset < end; i++)
    {
      if (!TryReadVarInt(data, ref offset, end, out var recordLength) || recordLength < 0)
      {
        break;
      }

      var recordEnd = offset + recordLength;
      if (recordEnd > end)
      {
        break;
      }

      if (!TryReadByte(data, ref offset, recordEnd, out _)) // attributes
      {
        break;
      }

      if (!TryReadVarLong(data, ref offset, recordEnd, out _)) // timestampDelta
      {
        break;
      }

      if (!TryReadVarInt(data, ref offset, recordEnd, out _)) // offsetDelta
      {
        break;
      }

      if (!TryReadBytesWithVarIntLength(data, ref offset, recordEnd, out _)) // key
      {
        break;
      }

      if (!TryReadBytesWithVarIntLength(data, ref offset, recordEnd, out var valueBytes)) // value
      {
        break;
      }

      if (!TryReadVarInt(data, ref offset, recordEnd, out var headersCount) || headersCount < 0)
      {
        break;
      }

      if (!SkipHeaders(data, ref offset, recordEnd, headersCount))
      {
        break;
      }

      if (valueBytes is { Length: > 0 })
      {
        ParseMetadataValue(valueBytes);
      }

      offset = recordEnd;
    }
  }

  static void ParseMetadataValue(byte[] valueBytes)
  {
    var offset = 0;
    var end = valueBytes.Length;

    if (!TryReadUnsignedVarInt(valueBytes, ref offset, end, out var frameVersion) || frameVersion != 1)
    {
      return;
    }

    if (!TryReadUnsignedVarInt(valueBytes, ref offset, end, out var apiKey))
    {
      return;
    }

    if (!TryReadUnsignedVarInt(valueBytes, ref offset, end, out var version))
    {
      return;
    }

    if (apiKey == TopicRecordApiKey)
    {
      ParseTopicRecord(valueBytes, ref offset, end, version);
      return;
    }

    if (apiKey == PartitionRecordApiKey)
    {
      ParsePartitionRecord(valueBytes, ref offset, end, version);
    }
  }

  static void ParseTopicRecord(byte[] data, ref int offset, int end, int version)
  {
    if (!TryReadCompactString(data, ref offset, end, out var topicName))
    {
      return;
    }

    if (!TryReadUuid(data, ref offset, end, out var topicIdBytes))
    {
      return;
    }

    if (!SkipTaggedFields(data, ref offset, end))
    {
      return;
    }

    if (version != 0)
    {
      return;
    }

    var metadata = new TopicMetadata(topicName, topicIdBytes);
    TopicsByName[topicName] = metadata;
    TopicsById[metadata.TopicIdHex] = metadata;
  }

  static void ParsePartitionRecord(byte[] data, ref int offset, int end, int version)
  {
    if (!TryReadInt32(data, ref offset, end, out var partitionId))
    {
      return;
    }

    if (!TryReadUuid(data, ref offset, end, out var topicIdBytes))
    {
      return;
    }

    if (!TryReadCompactInt32Array(data, ref offset, end, out var replicas))
    {
      return;
    }

    if (!TryReadCompactInt32Array(data, ref offset, end, out var isr))
    {
      return;
    }

    if (!TryReadCompactInt32Array(data, ref offset, end, out _)) // removingReplicas
    {
      return;
    }

    if (!TryReadCompactInt32Array(data, ref offset, end, out _)) // addingReplicas
    {
      return;
    }

    if (!TryReadInt32(data, ref offset, end, out var leader))
    {
      return;
    }

    if (!TryReadInt32(data, ref offset, end, out var leaderEpoch))
    {
      return;
    }

    if (!TryReadInt32(data, ref offset, end, out _)) // partitionEpoch
    {
      return;
    }

    if (version >= 1 && !TryReadCompactUuidArray(data, ref offset, end, out _))
    {
      return;
    }

    if (!TryReadTaggedFields(data, ref offset, end, out var taggedFields))
    {
      return;
    }

    var eligibleLeaderReplicas = Array.Empty<int>();
    var lastKnownElr = Array.Empty<int>();

    if (version >= 2)
    {
      if (taggedFields.TryGetValue(1, out var elrBytes))
      {
        if (!TryParseCompactNullableInt32Array(elrBytes, out eligibleLeaderReplicas))
        {
          eligibleLeaderReplicas = Array.Empty<int>();
        }
      }

      if (taggedFields.TryGetValue(2, out var lastKnownElrBytes))
      {
        if (!TryParseCompactNullableInt32Array(lastKnownElrBytes, out lastKnownElr))
        {
          lastKnownElr = Array.Empty<int>();
        }
      }
    }

    var topicIdHex = Convert.ToHexString(topicIdBytes);
    if (!TopicsById.TryGetValue(topicIdHex, out var topic))
    {
      return;
    }

    topic.UpsertPartition(new PartitionMetadata(partitionId, leader, leaderEpoch, replicas, isr, eligibleLeaderReplicas, lastKnownElr));
  }

  static bool SkipHeaders(byte[] data, ref int offset, int end, int headersCount)
  {
    for (var i = 0; i < headersCount; i++)
    {
      if (!TryReadBytesWithVarIntLength(data, ref offset, end, out _)) // header key bytes
      {
        return false;
      }

      if (!TryReadBytesWithVarIntLength(data, ref offset, end, out _)) // header value bytes
      {
        return false;
      }
    }

    return true;
  }

  static bool TryReadTaggedFields(byte[] data, ref int offset, int end, out Dictionary<int, byte[]> fields)
  {
    fields = [];

    if (!TryReadUnsignedVarInt(data, ref offset, end, out var fieldCount))
    {
      return false;
    }

    for (var i = 0; i < fieldCount; i++)
    {
      if (!TryReadUnsignedVarInt(data, ref offset, end, out var tag))
      {
        return false;
      }

      if (!TryReadUnsignedVarInt(data, ref offset, end, out var size))
      {
        return false;
      }

      if (size < 0 || offset + size > end)
      {
        return false;
      }

      var bytes = new byte[size];
      data.AsSpan(offset, size).CopyTo(bytes);
      fields[tag] = bytes;
      offset += size;
    }

    return true;
  }

  static bool SkipTaggedFields(byte[] data, ref int offset, int end)
  {
    return TryReadTaggedFields(data, ref offset, end, out _);
  }

  static bool TryReadCompactString(byte[] data, ref int offset, int end, out string value)
  {
    value = string.Empty;
    if (!TryReadUnsignedVarInt(data, ref offset, end, out var encodedLength) || encodedLength <= 0)
    {
      return false;
    }

    var length = encodedLength - 1;
    if (offset + length > end)
    {
      return false;
    }

    value = Encoding.UTF8.GetString(data, offset, length);
    offset += length;
    return true;
  }

  static bool TryReadCompactInt32Array(byte[] data, ref int offset, int end, out int[] values)
  {
    values = [];
    if (!TryReadUnsignedVarInt(data, ref offset, end, out var encodedLength) || encodedLength <= 0)
    {
      return false;
    }

    var length = encodedLength - 1;
    values = new int[length];
    for (var i = 0; i < length; i++)
    {
      if (!TryReadInt32(data, ref offset, end, out values[i]))
      {
        return false;
      }
    }

    return true;
  }

  static bool TryReadCompactUuidArray(byte[] data, ref int offset, int end, out byte[][] values)
  {
    values = [];
    if (!TryReadUnsignedVarInt(data, ref offset, end, out var encodedLength) || encodedLength <= 0)
    {
      return false;
    }

    var length = encodedLength - 1;
    values = new byte[length][];
    for (var i = 0; i < length; i++)
    {
      if (!TryReadUuid(data, ref offset, end, out var uuid))
      {
        return false;
      }

      values[i] = uuid;
    }

    return true;
  }

  static bool TryParseCompactNullableInt32Array(byte[] taggedBytes, out int[] values)
  {
    values = [];
    var offset = 0;
    var end = taggedBytes.Length;
    if (!TryReadUnsignedVarInt(taggedBytes, ref offset, end, out var encodedLength))
    {
      return false;
    }

    if (encodedLength == 0)
    {
      values = [];
      return true;
    }

    var length = encodedLength - 1;
    var parsed = new int[length];
    for (var i = 0; i < length; i++)
    {
      if (!TryReadInt32(taggedBytes, ref offset, end, out parsed[i]))
      {
        return false;
      }
    }

    values = parsed;
    return offset == end;
  }

  static bool TryReadBytesWithVarIntLength(byte[] data, ref int offset, int end, out byte[]? value)
  {
    value = null;
    if (!TryReadVarInt(data, ref offset, end, out var length))
    {
      return false;
    }

    if (length < 0)
    {
      value = null;
      return true;
    }

    if (offset + length > end)
    {
      return false;
    }

    value = new byte[length];
    data.AsSpan(offset, length).CopyTo(value);
    offset += length;
    return true;
  }

  static bool TryReadInt32(byte[] data, ref int offset, int end, out int value)
  {
    value = 0;
    if (offset + 4 > end)
    {
      return false;
    }

    value = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
    offset += 4;
    return true;
  }

  static bool TryReadUuid(byte[] data, ref int offset, int end, out byte[] uuid)
  {
    uuid = [];
    if (offset + 16 > end)
    {
      return false;
    }

    uuid = new byte[16];
    data.AsSpan(offset, 16).CopyTo(uuid);
    offset += 16;
    return true;
  }

  static bool TryReadByte(byte[] data, ref int offset, int end, out byte value)
  {
    value = 0;
    if (offset + 1 > end)
    {
      return false;
    }

    value = data[offset++];
    return true;
  }

  static bool TryReadUnsignedVarInt(byte[] data, ref int offset, int end, out int value)
  {
    value = 0;
    var shift = 0;
    while (offset < end && shift <= 28)
    {
      var b = data[offset++];
      value |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0)
      {
        return true;
      }

      shift += 7;
    }

    return false;
  }

  static bool TryReadVarInt(byte[] data, ref int offset, int end, out int value)
  {
    value = 0;
    if (!TryReadUnsignedVarInt(data, ref offset, end, out var raw))
    {
      return false;
    }

    value = (raw >> 1) ^ -(raw & 1);
    return true;
  }

  static bool TryReadVarLong(byte[] data, ref int offset, int end, out long value)
  {
    value = 0;
    long raw = 0;
    var shift = 0;
    while (offset < end && shift <= 63)
    {
      var b = data[offset++];
      raw |= (long)(b & 0x7F) << shift;
      if ((b & 0x80) == 0)
      {
        value = (raw >> 1) ^ -(raw & 1);
        return true;
      }

      shift += 7;
    }

    return false;
  }
}

public class TopicMetadata(string topicName, byte[] topicId)
{
  readonly Dictionary<int, PartitionMetadata> _partitions = [];

  public string TopicName { get; } = topicName;
  public byte[] TopicId { get; } = topicId;
  public string TopicIdHex => Convert.ToHexString(TopicId);
  public IReadOnlyList<PartitionMetadata> Partitions => _partitions.Values.OrderBy(p => p.PartitionId).ToList();

  public void UpsertPartition(PartitionMetadata partition)
  {
    _partitions[partition.PartitionId] = partition;
  }
}

public class PartitionMetadata(
  int partitionId,
  int leaderId,
  int leaderEpoch,
  IReadOnlyList<int> replicas,
  IReadOnlyList<int> isr,
  IReadOnlyList<int> eligibleLeaderReplicas,
  IReadOnlyList<int> lastKnownElr)
{
  public int PartitionId { get; } = partitionId;
  public int LeaderId { get; } = leaderId;
  public int LeaderEpoch { get; } = leaderEpoch;
  public IReadOnlyList<int> Replicas { get; } = replicas;
  public IReadOnlyList<int> Isr { get; } = isr;
  public IReadOnlyList<int> EligibleLeaderReplicas { get; } = eligibleLeaderReplicas;
  public IReadOnlyList<int> LastKnownElr { get; } = lastKnownElr;
}