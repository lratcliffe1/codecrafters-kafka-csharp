using System.Text;
using src.Requests.Base;

namespace src.Requests;

public static class SharedRequestParsers
{
  public static List<string> ParseTopicNamesFromCompactArray(RequestHeader requestHeader, byte[] request)
  {
    // Request header v2 includes a header TAG_BUFFER byte after client_id.
    // Body starts after: api_key(2) + api_version(2) + correlation_id(4) + client_id_len(2) + client_id + tagged_fields.
    var requestBodyOffset = 10 + requestHeader.ClientId.Length + 1;
    if (requestBodyOffset >= request.Length)
    {
      return [];
    }

    var offset = requestBodyOffset;
    if (!TryReadUnsignedVarInt(request, ref offset, out var encodedTopicsLength))
    {
      return [];
    }

    var topicsCount = encodedTopicsLength - 1;
    if (topicsCount <= 0)
    {
      return [];
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

    return topicNames;
  }

  public static bool TryReadCompactString(byte[] data, ref int offset, out string value)
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

  public static bool TrySkipTagBuffer(byte[] data, ref int offset)
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

  public static bool TryReadUnsignedVarInt(byte[] data, ref int offset, out int value)
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
