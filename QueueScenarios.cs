using Azure.Messaging.ServiceBus;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace premium_sb_samples
{
    public static class QueueScenarios
    {
        public static async Task DeleteAllScenarioQueuesAsync(string sbConnectionString)
        {
            List<string> queues = new List<string>()
            {
                Constants.SampleQueueNames.q_send_autottlmsg_dead_letter_set_defer,
                Constants.SampleQueueNames.q_send_autottlmsg_dead_letter_set_defer_complete,
                Constants.SampleQueueNames.q_send_defer,
                Constants.SampleQueueNames.q_send_receive,
                Constants.SampleQueueNames.q_send_receive_autottlmsg_dead_letter
            };
            foreach (var q in queues)
            {
                QueueRunner queueRunner = new QueueRunner(sbConnectionString, q, 0);
                await queueRunner.DeleteQueueAsync();
            }
        }

        public static async Task Q_Send_ReceiveAsync(string sbConnectionString)
        {
            Console.WriteLine("====== Scenario: Queue - Send Msg with TTL -> Receive it -> print messages======\n");
            int sampleMsgsCount = 10;

            // one day
            int messageTtl = 86400;
            QueueRunner queueRunner = new QueueRunner(sbConnectionString, Constants.SampleQueueNames.q_send_receive, sampleMsgsCount);
            await queueRunner.CleanUpQueueAsync();

            await queueRunner.SendSampleMessagesAsync(msgTtl: TimeSpan.FromSeconds(messageTtl));

            var sbClient = queueRunner.GetServiceBusClient();

            ServiceBusReceiver receiver = sbClient.CreateReceiver(queueRunner.QueueName);

            await queueRunner.PeekSampleMessagesAsync(receiver);
            await queueRunner.ReceiveSampleMessagesAsync(receiver);

            await receiver.DisposeAsync();

            queueRunner.PrintState();
            queueRunner.PrintReceivedMsgs();
            queueRunner.ClearAllCollections();

            Console.WriteLine("====== End: Queue - Send Msg with TTL -> Receive it -> print messages======\n");
        }

        public static async Task Q_Send_ScheduleAsync(string sbConnectionString)
        {
            Console.WriteLine("====== Scenario: Queue - Send Msg with TTL -> Receive it -> print messages======\n");
            int sampleMsgsCount = 10;

            // one day
            int oneDayInSeconds = 86400;
            QueueRunner queueRunner = new QueueRunner(sbConnectionString, Constants.SampleQueueNames.q_send_schedule, sampleMsgsCount);
            await queueRunner.CleanUpQueueAsync();

            await queueRunner.SendScheduleSampleMessagesAsync(msgAddTimeSpanToUtcNow: TimeSpan.FromSeconds(oneDayInSeconds));

            var sbClient = queueRunner.GetServiceBusClient();

            ServiceBusReceiver receiver = sbClient.CreateReceiver(queueRunner.QueueName);

            await queueRunner.PeekSampleMessagesAsync(receiver);
            await queueRunner.ReceiveSampleMessagesAsync(receiver);

            await receiver.DisposeAsync();

            queueRunner.PrintState();
            queueRunner.PrintReceivedMsgs();
            queueRunner.ClearAllCollections();

            Console.WriteLine("====== End: Queue - Send Msg with TTL -> Receive it -> print messages======\n");
        }

        public static async Task Q_Receive_Dead_Letter_Msgs_Async(string sbConnectionString)
        {
            Console.WriteLine("====== Scenario: Queue - Send Msg with TTL -> Receive it -> print messages======\n");
            int sampleMsgsCount = 10;

            QueueRunner queueRunner = new QueueRunner(sbConnectionString, Constants.SampleQueueNames.q_dead_letter_peeked_msgs, sampleMsgsCount);

            var sbClient = queueRunner.GetServiceBusClient();

            ServiceBusReceiver receiver = sbClient.CreateReceiver(queueRunner.QueueName, new ServiceBusReceiverOptions
            { SubQueue=SubQueue.DeadLetter});

            await queueRunner.ReceiveSampleMessagesAsync(receiver);

            await receiver.DisposeAsync();

            Console.WriteLine("====== End: Q_Receive_Dead_Letter_Msgs_Async======\n");
        }

        public static async Task Q_Send_Dead_Letter_A_Msg_Async(string sbConnectionString)
        {
            Console.WriteLine("====== Scenario: Queue - Send Msg with TTL -> Receive it -> print messages======\n");
            int sampleMsgsCount = 10;

            // one day
            int messageTtl = 86400;
            QueueRunner queueRunner = new QueueRunner(sbConnectionString, Constants.SampleQueueNames.q_dead_letter_peeked_msgs, sampleMsgsCount);
            await queueRunner.CleanUpQueueAsync();

            await queueRunner.SendSampleMessagesAsync(msgTtl: TimeSpan.FromSeconds(messageTtl));

            var sbClient = queueRunner.GetServiceBusClient();

            ServiceBusReceiver receiver = sbClient.CreateReceiver(queueRunner.QueueName);

            await queueRunner.PeekSampleMessagesAsync(receiver);
            await queueRunner.DeadLetterPeekedSampleMessagesAsync(receiver);

            await receiver.DisposeAsync();

            queueRunner.PrintState();
            queueRunner.PrintReceivedMsgs();
            queueRunner.ClearAllCollections();

            Console.WriteLine("====== End: Queue - Send Msg with TTL -> Receive it -> print messages======\n");
        }

        public static async Task Q_Dead_Letter_Deferred_Msgs_Async(string sbConnectionString)
        {
            Console.WriteLine("====== Scenario: Q_Dead_Letter_Deferred_Msgs_Async======\n");
            int sampleMsgsCount = 10;

            QueueRunner queueRunner = new QueueRunner(sbConnectionString, Constants.SampleQueueNames.q_send_defer, sampleMsgsCount);

            var sbClient = queueRunner.GetServiceBusClient();

            //ServiceBusReceiver receiver = sbClient.CreateReceiver(queueRunner.QueueName, new ServiceBusReceiverOptions() { ReceiveMode=ServiceBusReceiveMode.PeekLock});

            //await queueRunner.PeekSampleMessagesAsync(receiver);

            ServiceBusReceiver receiver = sbClient.CreateReceiver(queueRunner.QueueName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });

            await queueRunner.DeadLetterDeferredSampleMessagesAsync(receiver);

            await receiver.DisposeAsync();

            Console.WriteLine("====== End: Q_Dead_Letter_Deferred_Msgs_Async======\n");
        }

        public static async Task Q_Large_Msg_Send_ReceiveAsync(string sbConnectionString)
        {
            Console.WriteLine("====== Scenario: Queue - Send Msg with TTL -> Receive it -> print messages======\n");
            int sampleMsgsCount = 2;

            // one day
            int messageTtl = 86400;

            QueueRunner queueRunner = new QueueRunner(sbConnectionString, Constants.SampleQueueNames.q_large_msg_send_receive, sampleMsgsCount);
            await queueRunner.CleanUpQueueAsync();

            await queueRunner.SendSampleLargeMessagesAsync(msgTtl: TimeSpan.FromMinutes(messageTtl));

            var sbClient = queueRunner.GetServiceBusClient();

            ServiceBusReceiver receiver = sbClient.CreateReceiver(queueRunner.QueueName);

            await queueRunner.ReceiveLargeMessageAsync(receiver);

            await receiver.DisposeAsync();

            queueRunner.ClearAllCollections();

            Console.WriteLine("====== End: Queue - Send Msg with TTL -> Receive it -> print messages======\n");
        }

        /// <summary>
        /// Notes:
        /// #1 When a msg is in defer state ttl won't apply
        /// </summary>
        /// <param name="sbConnectionString">Service Bus Connection String</param>
        public static async Task Q_Send_DeferAsync(string sbConnectionString)
        {
            Console.WriteLine("====== Scenario: Queue - Send Msg with TTL -> Set deferred state for msgs -> print messages======\n");

            int sampleMsgsCount = 10;
            int messageTtl = 6000;
            QueueRunner queueRunner = new QueueRunner(sbConnectionString, Constants.SampleQueueNames.q_send_defer, sampleMsgsCount);

            await queueRunner.CleanUpQueueAsync();

            await queueRunner.SendSampleMessagesAsync(msgTtl: TimeSpan.FromSeconds(messageTtl));

            var sbClient = queueRunner.GetServiceBusClient();

            ServiceBusReceiver receiver = sbClient.CreateReceiver(queueRunner.QueueName);

            await queueRunner.ReceiveSampleMessagesAsync(receiver);

            await queueRunner.DeferMessagesAsync(receiver);

            queueRunner.PrintState();

            queueRunner.ClearAllCollections();

            await receiver.DisposeAsync();

            receiver = sbClient.CreateReceiver(queueRunner.QueueName);

            await queueRunner.ReadDeferMessagesAsync(receiver);

            await receiver.DisposeAsync();

            Console.WriteLine("====== End: Queue - Send Msg with TTL -> Set deferred state for msgs -> print messages======\n");
        }

        /// <summary>
        /// Notes:
        /// #1 When a msg is in defer state ttl won't apply
        /// </summary>
        /// <param name="sbConnectionString">Service Bus Connection String</param>
        public static async Task Q_Set_Defer_State_For_DeadLetterQueueAsync(string sbConnectionString)
        {
            Console.WriteLine("====== Scenario: Queue - Send Msg with TTL -> Set deferred state for msgs -> print messages======\n");

            QueueRunner queueRunner = new QueueRunner(sbConnectionString, Constants.SampleQueueNames.q_send_defer, 10);

            var sbClient = queueRunner.GetServiceBusClient();

            ServiceBusReceiver receiver = sbClient.CreateReceiver(queueRunner.QueueName, new ServiceBusReceiverOptions() { SubQueue=SubQueue.DeadLetter});

            await queueRunner.ReceiveSampleMessagesAsync(receiver);

            await queueRunner.DeferMessagesAsync(receiver);

            queueRunner.PrintState();

            queueRunner.ClearAllCollections();

            await receiver.DisposeAsync();

            receiver = sbClient.CreateReceiver(queueRunner.QueueName, new ServiceBusReceiverOptions() { SubQueue = SubQueue.DeadLetter });

            await queueRunner.ReadDeferMessagesAsync(receiver);

            await receiver.DisposeAsync();

            Console.WriteLine("====== End: Queue - Send Msg with TTL -> Set deferred state for msgs -> print messages======\n");
        }

        /// <summary>
        /// </summary>
        /// <param name="sbConnectionString"></param>
        /// <returns></returns>
        public static async Task Q_Send_AutoMsgExpiry_DeadLetterAsync(string sbConnectionString)
        {
            Console.WriteLine("====== Scenario: Queue - Send Msg with TTL -> Wait for TTL time tigger on the server side -> Print dead letter queue msgs ======\n");

            int sampleMsgsCount = 10;
            int messageTtl = 30;
            string queueName = Constants.SampleQueueNames.q_send_receive_autottlmsg_dead_letter;

            QueueRunner queueRunner = new QueueRunner(sbConnectionString, queueName, sampleMsgsCount);

            await queueRunner.CleanUpQueueAsync();

            await queueRunner.SendSampleMessagesAsync(msgTtl: TimeSpan.FromSeconds(messageTtl));

            Console.WriteLine($"\nWaiting for {2 * messageTtl} seconds. Actual TTL is {messageTtl} seconds. Note TTL is a background job, it may have slight delay than the original ttl value hence the wait time is double.\n");

            System.Threading.Thread.Sleep(TimeSpan.FromSeconds(2 * messageTtl));

            var sbClient = queueRunner.GetServiceBusClient();

            ServiceBusReceiver receiver = sbClient.CreateReceiver(queueName, new ServiceBusReceiverOptions() { SubQueue = SubQueue.DeadLetter });

            await queueRunner.PeekDeadLetterSampleMessagesAsync(receiver);

            await receiver.DisposeAsync();

            queueRunner.PrintDeadLetterMsgs();

            Console.WriteLine("\n====== End: Queue - Send Msg with TTL -> Wait for TTL time trigger on the server side -> Print dead letter queue msgs ======");

            queueRunner.ClearAllCollections();
        }

        /// <summary>
        /// </summary>
        /// <param name="sbConnectionString"></param>
        /// <returns></returns>
        public static async Task Q_Send_AutoMsgExpiry_DeadLetter_SetDeferStateAsync(string sbConnectionString)
        {
            Console.WriteLine("====== Scenario: Queue - Send Msg with TTL -> Wait for TTL time trigger on the server side -> Print dead letter queue msgs -> Set DeferState ======\n");

            int sampleMsgsCount = 10;
            int messageTtl = 30;
            string queueName = Constants.SampleQueueNames.q_send_autottlmsg_dead_letter_set_defer;
            QueueRunner queueRunner = new QueueRunner(sbConnectionString, queueName, sampleMsgsCount);
            await queueRunner.CleanUpQueueAsync();
            await queueRunner.SendSampleMessagesAsync(msgTtl: TimeSpan.FromSeconds(messageTtl));

            Console.WriteLine($"\nWaiting for {2 * messageTtl} seconds. Actual TTL is {messageTtl} seconds. Note TTL is a background job, it may have slight delay than the original ttl value hence the wait time is double.\n");

            System.Threading.Thread.Sleep(TimeSpan.FromSeconds(2 * messageTtl));

            var sbClient = queueRunner.GetServiceBusClient();
            ServiceBusReceiver receiver = sbClient.CreateReceiver(queueName, new ServiceBusReceiverOptions() { SubQueue = SubQueue.DeadLetter });

            await queueRunner.PeekDeadLetterSampleMessagesAsync(receiver);
            await receiver.DisposeAsync();
            queueRunner.PrintDeadLetterMsgs();
            queueRunner.ClearAllCollections();

            receiver = sbClient.CreateReceiver(queueName, new ServiceBusReceiverOptions() { SubQueue = SubQueue.DeadLetter });
            await queueRunner.ReceiveSampleMessagesAsync(receiver);
            await queueRunner.DeferMessagesAsync(receiver);
            await receiver.DisposeAsync();

            receiver = sbClient.CreateReceiver(queueName, new ServiceBusReceiverOptions() { SubQueue = SubQueue.DeadLetter });
            await queueRunner.PeekDeadLetterSampleMessagesAsync(receiver);
            await receiver.DisposeAsync();
            queueRunner.PrintDeadLetterMsgs();

            Console.WriteLine("\n====== End: Queue - Send Msg with TTL -> Wait for TTL time trigger on the server side -> Print dead letter queue msgs -> Set DeferState ======");
        }

        /// <summary>
        /// </summary>
        /// <param name="sbConnectionString"></param>
        /// <returns></returns>
        public static async Task Q_Complete_DeadLetter_Deferrred_Msgs_Async(string sbConnectionString, string queueName, int msgCount)
        {
            Console.WriteLine("====== Scenario: Q_Complete_DeadLetter_Deferrred_Msgs_Async. ======\n");

            QueueRunner queueRunner = new QueueRunner(sbConnectionString, queueName, msgCount);
           
            var sbClient = queueRunner.GetServiceBusClient();
            ServiceBusReceiver receiver = sbClient.CreateReceiver(queueName, new ServiceBusReceiverOptions() { SubQueue = SubQueue.DeadLetter });

            await queueRunner.PeekDeadLetterSampleMessagesAsync(receiver);
            await queueRunner.CompletedDeferredMsgsAsync(receiver);

            queueRunner.ClearAllCollections();

            await queueRunner.PeekDeadLetterSampleMessagesAsync(receiver);
            queueRunner.PrintDeadLetterMsgs();

            Console.WriteLine("\n====== Q_Complete_DeadLetter_Deferrred_Msgs_Async. ======");
        }

        /// <summary>
        /// </summary>
        /// <param name="sbConnectionString"></param>
        /// <returns></returns>
        public static async Task Q_Send_AutoMsgExpiry_DeadLetter_SetDeferState_CompleteAsync(string sbConnectionString)
        {
            Console.WriteLine("====== Scenario: Queue - Send Msg with TTL -> Wait for TTL time trigger on the server side -> Print dead letter queue msgs -> Set DeferState -> Complete it. ======\n");

            int sampleMsgsCount = 10;
            int messageTtl = 30;
            string queueName = Constants.SampleQueueNames.q_send_autottlmsg_dead_letter_set_defer_complete;
            QueueRunner queueRunner = new QueueRunner(sbConnectionString, queueName, sampleMsgsCount);
            await queueRunner.CleanUpQueueAsync();
            await queueRunner.SendSampleMessagesAsync(msgTtl: TimeSpan.FromSeconds(messageTtl));

            Console.WriteLine($"\nWaiting for {2 * messageTtl} seconds. Actual TTL is {messageTtl} seconds. Note TTL is a background job, it may have slight delay than the original ttl value hence the wait time is double.\n");

            System.Threading.Thread.Sleep(TimeSpan.FromSeconds(2 * messageTtl));

            var sbClient = queueRunner.GetServiceBusClient();
            ServiceBusReceiver receiver = sbClient.CreateReceiver(queueName, new ServiceBusReceiverOptions() { SubQueue = SubQueue.DeadLetter });

            await queueRunner.ReceiveSampleMessagesAsync(receiver);
            await queueRunner.DeferMessagesAsync(receiver);
            await receiver.DisposeAsync();

            receiver = sbClient.CreateReceiver(queueName, new ServiceBusReceiverOptions() { SubQueue = SubQueue.DeadLetter });
            await queueRunner.PeekDeadLetterSampleMessagesAsync(receiver);
            await queueRunner.CompletedDeferredMsgsAsync(receiver);

            queueRunner.ClearAllCollections();

            await queueRunner.PeekDeadLetterSampleMessagesAsync(receiver);
            queueRunner.PrintDeadLetterMsgs();

            Console.WriteLine("\n====== End: Queue - Send Msg with TTL -> Wait for TTL time trigger on the server side -> Print dead letter queue msgs -> Set DeferState -> Complete it. ======");
        }
    }
}