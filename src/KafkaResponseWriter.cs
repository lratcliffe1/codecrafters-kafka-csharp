using System.Buffers.Binary;
using System.Text;

namespace src;

public class KafkaResponseWriter
{
  readonly byte[] _response;

  public int Offset { get; private set; } = 8;

  public KafkaResponseWriter(int messageSize, int correlationId)
  {
    _response = new byte[4 + messageSize];
    BinaryPrimitives.WriteInt32BigEndian(_response.AsSpan(0, 4), messageSize);
    BinaryPrimitives.WriteInt32BigEndian(_response.AsSpan(4, 4), correlationId);
  }

  public void WriteInt16(short value)
  {
    BinaryPrimitives.WriteInt16BigEndian(_response.AsSpan(Offset, 2), value);
    Offset += 2;
  }

  public void WriteInt32(int value)
  {
    BinaryPrimitives.WriteInt32BigEndian(_response.AsSpan(Offset, 4), value);
    Offset += 4;
  }

  public void WriteByte(byte value)
  {
    _response[Offset++] = value;
  }

  public void WriteBytes(byte[] value)
  {
    value.CopyTo(_response.AsSpan(Offset));
    Offset += value.Length;
  }

  public void WriteTagBufferEmpty() => WriteByte(0);

  public void WriteCompactArrayLength(int elementCount) => WriteByte((byte)(elementCount + 1));

  public void WriteCompactString(string value)
  {
    var bytes = Encoding.UTF8.GetBytes(value);
    WriteByte((byte)(bytes.Length + 1));
    bytes.CopyTo(_response.AsSpan(Offset));
    Offset += bytes.Length;
  }

  public void Advance(int bytesCount) => Offset += bytesCount;

  public byte[] ToArray() => _response;
}
