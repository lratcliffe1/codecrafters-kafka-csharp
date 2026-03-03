using src;

const int Port = 9092;
ClusterMetadata.LoadMetadata();
var server = new KafkaServer(Port);
await server.RunAsync();
