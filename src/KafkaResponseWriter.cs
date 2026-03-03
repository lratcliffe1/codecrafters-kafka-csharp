using System.Buffers.Binary;
using System.Text;

namespace src;

public class KafkaResponseWriter
{
  readonly List<byte> _response;

  public int Offset { get; private set; } = 8;

  public KafkaResponseWriter(int correlationId)
  {
    _response = new List<byte>(64);
    _response.AddRange(new byte[8]);

    Span<byte> correlationIdBytes = stackalloc byte[4];
    BinaryPrimitives.WriteInt32BigEndian(correlationIdBytes, correlationId);
    _response[4] = correlationIdBytes[0];
    _response[5] = correlationIdBytes[1];
    _response[6] = correlationIdBytes[2];
    _response[7] = correlationIdBytes[3];
  }

  public void WriteByte(byte value)
  {
    _response.Add(value);
    Offset++;
  }

  public void WriteInt16(short value)
  {
    Span<byte> bytes = stackalloc byte[2];
    BinaryPrimitives.WriteInt16BigEndian(bytes, value);
    _response.Add(bytes[0]);
    _response.Add(bytes[1]);
    Offset += 2;
  }

  public void WriteInt32(int value)
  {
    Span<byte> bytes = stackalloc byte[4];
    BinaryPrimitives.WriteInt32BigEndian(bytes, value);
    _response.Add(bytes[0]);
    _response.Add(bytes[1]);
    _response.Add(bytes[2]);
    _response.Add(bytes[3]);
    Offset += 4;
  }

  public void WriteBytes(byte[] value)
  {
    _response.AddRange(value);
    Offset += value.Length;
  }

  public void WriteTagBufferEmpty() => WriteByte(0);

  public void WriteCompactArrayLength(int elementCount) => WriteByte((byte)(elementCount + 1));

  public void WriteCompactString(string value)
  {
    var bytes = Encoding.UTF8.GetBytes(value);
    WriteByte((byte)(bytes.Length + 1));
    WriteBytes(bytes);
  }

  public void Advance(int bytesCount)
  {
    _response.AddRange(new byte[bytesCount]);
    Offset += bytesCount;
  }

  public byte[] ToArray()
  {
    var response = _response.ToArray();
    BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(0, 4), response.Length - 4);
    return response;
  }
}
