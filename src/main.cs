using src;

const int Port = 9092;
var server = new KafkaServer(Port);
await server.RunAsync();
