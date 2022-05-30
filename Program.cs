
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Configuration;

namespace RecoverDeadLetterQueuesGeneric
{
    class Program
    {
        static void Main(string[] args)
        {
            RecoverQueue();
            PurgeQueue();
            RecoverTopic();
            PurgeMessagesFromSubscription();
        }

        static void PurgeMessagesFromSubscription()
        {
            var cnnStr = ConfigurationManager.ConnectionStrings["carguero-hml"].ConnectionString;
            var topics = new Dictionary<string, string> {
                { "topic_analise_cadastral", "backoffice" }
            };

            foreach (var topic in topics)
            {
                Console.SetCursorPosition(1, 1);
                Console.Write(new string(' ', Console.WindowWidth));
                Console.SetCursorPosition(1, 1);
                Console.WriteLine($"topic: {topic.Key}, subs: {topic.Value}");
                var topicClient = TopicClient.CreateFromConnectionString(cnnStr, topic.Key);
                var factory = MessagingFactory.CreateFromConnectionString(cnnStr);
                var subscriptionPath = SubscriptionClient.FormatDeadLetterPath(topic.Key, topic.Value);
                var dlqReceiver = factory.CreateMessageReceiver(subscriptionPath, ReceiveMode.PeekLock);
           
                while (true)
                {
                    var msg = dlqReceiver.Receive();
                    if (msg == null)
                        break;

                    Console.SetCursorPosition(1, 3);
                    Console.Write(new string(' ', Console.WindowWidth));
                    Console.SetCursorPosition(1, 3);
                    Console.WriteLine($"msg content: {msg.MessageId}");

                    try
                    {
                        // Send message to queue
                        var bm = msg.Clone();
                        bm.AbandonAsync();
                        msg.Complete();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
            Console.SetCursorPosition(1, 5);
            Console.WriteLine("Press any key to exit!");
            Console.Read();
        }

        private static void RecoverTopic()
        {
            var cnnStr = ConfigurationManager.ConnectionStrings["carguero-aws"].ConnectionString;
            var topics = new Dictionary<string, string> {
                //{"topic_analise_cadastral", "backoffice" }
                //{"all_events","hub_notification" }
                //{"all_events","webhook_relay" }
                //{"all_events","aprovacao_automatica" }
                //{"all_events","acompanhamento_viagem" }
                //{"all_events","audit" }
                //{"topic_webhook","topic_webhook-amaggi" }
                //{"all_events","troca_nota_classificadora" }

            };

            foreach (var topic in topics)
            {
                Console.SetCursorPosition(1, 1);
                Console.Write(new string(' ', Console.WindowWidth));
                Console.SetCursorPosition(1, 1);
                Console.WriteLine($"topic: {topic.Key}, subs: {topic.Value}");
                var topicClient = TopicClient.CreateFromConnectionString(cnnStr, topic.Key);
                var factory = MessagingFactory.CreateFromConnectionString(cnnStr);
                //var deadLetterPath = SubscriptionClient.FormatDeadLetterPath(topic.Key, topic.Value);
                //var dlqReceiver = factory.CreateMessageReceiver(deadLetterPath, ReceiveMode.PeekLock);

                var subscriptionPath = SubscriptionClient.FormatDeadLetterPath(topic.Key, topic.Value);
                var dlqReceiver = factory.CreateMessageReceiver(subscriptionPath, ReceiveMode.PeekLock);

                while (true)
                {
                    var msg = dlqReceiver.Receive();
                    if (msg == null)
                        break;
                    Console.SetCursorPosition(1, 3);
                    Console.Write(new string(' ', Console.WindowWidth));
                    Console.SetCursorPosition(1, 3);
                    Console.WriteLine($"msg content: {msg.MessageId}");

                    try
                    {
                        // Send message to queue
                        var bm = msg.Clone();
                        topicClient.Send(bm);
                        msg.Complete();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
            Console.SetCursorPosition(1, 5);
            Console.WriteLine("Press any key to exit!");
            Console.Read();
        }

        private static void RecoverQueue()
        {
            var cnnStr = ConfigurationManager.ConnectionStrings["carguero-aws"].ConnectionString;
            var queues = ConfigurationManager.AppSettings["queues"].Split('|');

            foreach (var queueName in queues)
            {
                Console.SetCursorPosition(1, 1);
                Console.Write(new string(' ', Console.WindowWidth));
                Console.SetCursorPosition(1, 1);
                Console.WriteLine($"queue name: {queueName}");
                var queue = QueueClient.CreateFromConnectionString(cnnStr, queueName);
                var factory = MessagingFactory.CreateFromConnectionString(cnnStr);
                var deadLetterPath = QueueClient.FormatDeadLetterPath(queueName);
                var dlqReceiver = factory.CreateMessageReceiver(deadLetterPath, ReceiveMode.PeekLock);

                while (true)
                {
                    var msg = dlqReceiver.Receive();
                    if (msg == null)
                        break;
                    Console.SetCursorPosition(1, 3);
                    Console.Write(new string(' ', Console.WindowWidth));
                    Console.SetCursorPosition(1, 3);
                    Console.WriteLine($"msg content: {msg.MessageId}");
                    queue.Send(msg.Clone());
                    msg.Complete();
                }
            }
            Console.SetCursorPosition(1, 5);
            Console.WriteLine("Press any key to exit!");
            Console.Read();
        }

        private static void PurgeQueue()
        {
            var cnnStr = ConfigurationManager.ConnectionStrings["carguero-prd"].ConnectionString;
            var queues = ConfigurationManager.AppSettings["queues"].Split('|');

            foreach (var queueName in queues)
            {
                Console.SetCursorPosition(1, 1);
                Console.Write(new string(' ', Console.WindowWidth));
                Console.SetCursorPosition(1, 1);
                Console.WriteLine($"queue name: {queueName}");
                var queue = QueueClient.CreateFromConnectionString(cnnStr, queueName);
                var factory = MessagingFactory.CreateFromConnectionString(cnnStr);
                var deadLetterPath = QueueClient.FormatDeadLetterPath(queueName);
                var dlqReceiver = factory.CreateMessageReceiver(deadLetterPath, ReceiveMode.PeekLock);

                while (true)
                {
                    var msg = dlqReceiver.Receive();
                    if (msg == null)
                        break;
                    Console.SetCursorPosition(1, 3);
                    Console.Write(new string(' ', Console.WindowWidth));
                    Console.SetCursorPosition(1, 3);
                    Console.WriteLine($"msg content: {msg.MessageId}");
            
                    msg.Abandon();
                    msg.Complete();
                }
            }
            Console.SetCursorPosition(1, 5);
            Console.WriteLine("Press any key to exit!");
            Console.Read();
        }

        protected static BrokeredMessage CreateBrokeredMessage(string id, object message)
        {
            var bm = string.IsNullOrWhiteSpace(id) ? new BrokeredMessage(message) : new BrokeredMessage(message) { MessageId = id };
            if (bm.Size > 250 * 1024)
                throw new ArgumentOutOfRangeException($"MessageId {bm.MessageId} is too large. Size in bytes equals to {bm.Size}");
            return bm;
        }
    }
}
