using System;
using System.Text;
using System.Threading.Tasks;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace AzureEventHubProducer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console(theme: AnsiConsoleTheme.Literate)
                .CreateLogger();
            logger.Information(
                "Testando o envio de mensagens para um Event Hub no Azure");

            if (args.Length < 3)
            {
                logger.Error(
                    "Informe ao menos 3 parametros: " +
                    "no primeiro a string de conexao com o Azure Event Hubs, " +
                    "no segundo o nome do Event Hub que recebera as mensagens, " +
                    "ja no terceiro em diante a serem enviado a serem " +
                    "enviadas ao Event Hub no Azure...");
                return;
            }

            string connectionString = args[0];
            string eventHub = args[1];

            logger.Information($"Event Hub = {eventHub}");

            EventHubProducerClient producerClient = null;
            try
            {
                producerClient = new EventHubProducerClient(connectionString, eventHub);
                using var eventBatch = await producerClient.CreateBatchAsync();
                logger.Information("Gerando o Batch para envio dos eventos...");

                for (int i = 2; i < args.Length; i++)
                {
                    if (eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(args[i]))))
                        logger.Information($"Evento adicionado ao Batch | Posicao: {i} = {args[i]}");
                    else
                        throw new Exception($"O tamanho em dados do evento indicado na posicao {i} "+
                            "e superior ao limite suportado e nao sera enviado!");
                }

                await producerClient.SendAsync(eventBatch);
                logger.Information("Concluido o envio dos eventos!");
            }
            catch (Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
            finally
            {
                if (producerClient is not null)
                {
                    await producerClient.DisposeAsync();
                    logger.Information(
                        "Conexao com o Azure Event Hubs finalizada!");
                }
            }
        }
    }
}