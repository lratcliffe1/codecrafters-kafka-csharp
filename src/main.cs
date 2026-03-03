using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using src;

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
        var kafkaRequest = RequestParcer.ParseRequest(messageSize, requestBuffer);
        if (kafkaRequest == null)
        {
          Console.Error.WriteLine($"Unknown request type: {kafkaRequest?.RequestHeader.ApiKey}");
          continue;
        }

        var response = ResponseBuilder.BuildResponse(kafkaRequest!);

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
