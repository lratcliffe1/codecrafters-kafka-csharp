using System.Buffers.Binary;
using System.Text;
using src.Requests.Base;

namespace src.Requests;

public static class SharedRequestParsers
{
  public static int ReadRequestBodyOffset(RequestHeader requestHeader)
  {
    return 10 + requestHeader.ClientId.Length + 1;
  }

  public static int ReadCompactArrayCount(byte[] data, ref int offset)
  {
    return ReadUnsignedVarInt(data, ref offset) - 1;
  }

  public static string ReadCompactString(byte[] data, ref int offset)
  {
    var length = ReadUnsignedVarInt(data, ref offset) - 1;
    var value = Encoding.UTF8.GetString(data.AsSpan(offset, length));
    offset += length;
    return value;
  }

  public static void SkipTagBuffer(byte[] data, ref int offset)
  {
    var tagCount = ReadUnsignedVarInt(data, ref offset);
    for (var i = 0; i < tagCount; i++)
    {
      ReadUnsignedVarInt(data, ref offset); // tag id
      var tagSize = ReadUnsignedVarInt(data, ref offset);
      offset += tagSize;
    }
  }

  public static int ReadUnsignedVarInt(byte[] data, ref int offset)
  {
    var value = 0;
    var shift = 0;
    while (true)
    {
      var current = data[offset++];
      value |= (current & 0x7F) << shift;
      if ((current & 0x80) == 0)
      {
        return value;
      }

      shift += 7;
    }
  }

  public static int ReadInt32(byte[] data, ref int offset)
  {
    var value = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
    offset += 4;
    return value;
  }

  public static byte[] ReadUuid(byte[] data, ref int offset)
  {
    var value = data.AsSpan(offset, 16).ToArray();
    offset += 16;
    return value;
  }
}
