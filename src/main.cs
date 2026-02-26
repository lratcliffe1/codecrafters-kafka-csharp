using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

const int Port = 9092;

var listener = new TcpListener(IPAddress.Any, Port);
listener.Start();

while (true)
{
  try
  {
    var client = await listener.AcceptTcpClientAsync();
    _ = HandleClientAsync(client);
  }
  catch (Exception ex)
  {
    Console.Error.WriteLine($"Error accepting client: {ex.Message}");
  }
}

static async Task HandleClientAsync(TcpClient client)
{
  using (client)
  await using (var stream = client.GetStream())
  {
    var sizeBuffer = new byte[4];

    try
    {
      while (true)
      {
        var gotSize = await TryReadExactlyAsync(stream, sizeBuffer);
        if (!gotSize)
        {
          break;
        }

        var messageSize = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);
        if (messageSize < 0)
        {
          Console.Error.WriteLine($"Invalid message_size received: {messageSize}");
          break;
        }

        var requestBuffer = new byte[messageSize];
        var gotRequestBody = await TryReadExactlyAsync(stream, requestBuffer);
        if (!gotRequestBody)
        {
          break;
        }
        var request = ParseRequest(messageSize, requestBuffer);

        var response = BuildResponse(request.CorrelationId);
        
        await stream.WriteAsync(response);
        await stream.FlushAsync();
      }
    }
    catch (Exception ex)
    {
      Console.Error.WriteLine($"Error handling client: {ex.Message}");
    }
  }
}

static async Task<bool> TryReadExactlyAsync(NetworkStream stream, byte[] buffer)
{
  var totalRead = 0;
  while (totalRead < buffer.Length)
  {
    var bytesRead = await stream.ReadAsync(buffer.AsMemory(totalRead));
    if (bytesRead == 0)
    {
      return false;
    }

    totalRead += bytesRead;
  }

  return true;
}

static KafkaMessage ParseRequest(int messageSize, byte[] request)
{
  var apiKey = BinaryPrimitives.ReadInt16BigEndian(request.AsSpan(0, 2));
  var apiVersion = BinaryPrimitives.ReadInt16BigEndian(request.AsSpan(2, 2));
  var correlationId = BinaryPrimitives.ReadInt32BigEndian(request.AsSpan(4, 4));

  return new KafkaMessage(messageSize, apiKey, apiVersion, correlationId);
}

static byte[] BuildResponse(int correlationId)
{
  const int responseBodySize = 4;
  var response = new byte[4 + responseBodySize];

  BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(0, 4), responseBodySize);
  BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(4, 4), correlationId);

  return response;
}

class KafkaMessage(int messageSize, int apiKey, int apiVersion, int correlationId)
{
  public int MessageSize { get; set; } = messageSize;
  public int ApiKey { get; set; } = apiKey;
  public int ApiVersion { get; set; } = apiVersion;
  public int CorrelationId { get; set; } = correlationId;
}
