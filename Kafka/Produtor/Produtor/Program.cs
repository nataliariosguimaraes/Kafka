using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaProducerExample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string bootstrapServers = "localhost:9091"; // Endereço e porta do servidor do Kafka
            string topicName = "topic1"; // Nome do tópico onde as mensagens serão publicadas

            // Configuração do produtor
            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            // Cria o produtor
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    // Loop para enviar mensagens
                    while (true)
                    {
                        Console.Write("Digite uma mensagem ('sair' para sair): ");
                        var message = Console.ReadLine();

                        if (message.ToLower() == "sair")
                            break;

                        // Cria a mensagem para publicação no tópico
                        var kafkaMessage = new Message<Null, string>
                        {
                            Value = message
                        };

                        // Publica a mensagem no tópico de forma assíncrona
                        var deliveryResult = await producer.ProduceAsync(topicName, kafkaMessage);

                        // Verifica se a mensagem foi enviada com sucesso
                        Console.WriteLine($"Mensagem enviada com sucesso: {deliveryResult.Value}");
                    }
                }
                catch (ProduceException<Null, string> ex)
                {
                    Console.WriteLine($"Erro ao enviar mensagem: {ex.Error.Reason}");
                }
            }
        }
    }
}
