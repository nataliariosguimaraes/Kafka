using System;
using System.Threading;
using Confluent.Kafka;

namespace KafkaConsumerExample
{
    class Program
    {
        static void Main(string[] args)
        {
            string bootstrapServers = "localhost:9091"; // Endereço e porta do servidor do Kafka
            string topicName = "topic1"; // Nome do tópico a ser consumido

            // Configuração do consumidor
            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "my-consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest, // Define a partir de onde o consumidor lerá as mensagens no tópico
                EnableAutoCommit = false // Desativa o commit automático, para que possamos controlar manualmente as confirmações
            };

            // Cria o consumidor
            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                // Inscreve o consumidor no tópico
                consumer.Subscribe(topicName);

                // Loop para receber mensagens
                try
                {
                    while (true)
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));

                        if (consumeResult != null)
                        {
                            // Processa a mensagem recebida
                            Console.WriteLine($"Mensagem recebida: {consumeResult.Message.Value}");

                            // Confirma a mensagem, informando ao Kafka que a mensagem foi processada com sucesso
                            consumer.Commit(consumeResult);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Caso ocorra alguma exceção ou o usuário encerre a aplicação
                    Console.WriteLine("Recebimento de mensagens cancelado.");
                }
                finally
                {
                    // Fecha o consumidor
                    consumer.Close();
                }
            }
        }
    }
}
