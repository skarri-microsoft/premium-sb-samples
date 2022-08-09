using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace premium_sb_samples
{
    public class QueueRunner
    {
        private string sbConnectionStr;
        private string queueName;
        private QueueProperties queueProperties;
        private ServiceBusAdministrationClient serviceBusAdministrationClient;
        private ServiceBusClient serviceBusClient;
        private int sampleMsgsCount;
        private List<ServiceBusReceivedMessage> peekedMsgs;
        private List<ServiceBusReceivedMessage> peekedDeadLetterMsgs;
        private List<ServiceBusReceivedMessage> receivedMsgs;

        public QueueRunner(string sbConnectionStr, string queueName, int sampleMsgsCount)
        {
            this.sbConnectionStr = sbConnectionStr;
            this.queueName = queueName;
            this.sampleMsgsCount = sampleMsgsCount;
        }

        public string QueueName
        { get { return queueName; } }

        // No retry limit and no exception will be thrown
        public async Task CreateQueueIfNotExistsAsync(bool requiresSession = false)
        {
            if (!await CheckIfQueueExitsAsync())
            {
                var options = new CreateQueueOptions(queueName)
                {
                    AutoDeleteOnIdle = TimeSpan.FromDays(7),
                    DefaultMessageTimeToLive = TimeSpan.FromSeconds(60),
                    DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(1),
                    EnableBatchedOperations = true,
                    DeadLetteringOnMessageExpiration = true,
                    EnablePartitioning = false,
                    ForwardDeadLetteredMessagesTo = null,
                    ForwardTo = null,
                    LockDuration = TimeSpan.FromSeconds(45),
                    MaxDeliveryCount = 10,
                    MaxSizeInMegabytes = 2048,
                    RequiresDuplicateDetection = true,
                    RequiresSession = requiresSession,
                    UserMetadata = "some metadata"
                };

                options.AuthorizationRules.Add(new SharedAccessAuthorizationRule("allClaims", new[] { AccessRights.Manage, AccessRights.Send, AccessRights.Listen }));
                var client = GetServiceBusAdministrationClient();
                while (true)
                {
                    try
                    {
                        Console.WriteLine($"Creating queue:{queueName}");
                        queueProperties = null;

                        queueProperties = await client.CreateQueueAsync(options);

                        if (queueProperties == null)
                        {
                            System.Threading.Thread.Sleep(TimeSpan.FromSeconds(2));
                        }
                        else
                        {
                            Console.WriteLine($"Queue: '{queueName}' created.");
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Creation fails if it's already exists or fails to create it. {ex.Message.ToString()}");
                        break;
                    }
                }
            }
        }

        public async Task DeleteQueueAsync()
        {
            if (await CheckIfQueueExitsAsync())
            {
                Console.WriteLine($"Deleting Queue: '{queueName}' ...");
                await GetServiceBusAdministrationClient().DeleteQueueAsync(queueName);
            }
        }

        // Batch send is not honoring TTL why?
        public async Task SendSampleMessagesAsync(TimeSpan msgTtl)
        {
            var sbClient = GetServiceBusClient();

            ServiceBusSender sender = sbClient.CreateSender(queueName);

            Queue<ServiceBusMessage> messages = new Queue<ServiceBusMessage>();
            Console.WriteLine($"Sending {sampleMsgsCount} messages to queue:'{queueName}'...");
            for (int i = 0; i < sampleMsgsCount; i++)
            {
                messages.Enqueue(new ServiceBusMessage($"message body: UTC time: {DateTime.UtcNow} - Item number: {i} ")
                { MessageId = Guid.NewGuid().ToString(), TimeToLive = msgTtl });
            }

            while (messages.Count > 0)
            {
                using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

                if (messageBatch.TryAddMessage(messages.Peek()))
                {
                    messages.Dequeue();
                }
                else
                {
                    // if the first message can't fit, then it is too large for the batch
                    throw new Exception($"Message {sampleMsgsCount - messages.Count} is too large and cannot be sent.");
                }

                // add as many messages as possible to the current batch
                while (messages.Count > 0 && messageBatch.TryAddMessage(messages.Peek()))
                {
                    messages.Dequeue();
                }

                await sender.SendMessagesAsync(messageBatch);
                Console.WriteLine($"Send to queue:'{sampleMsgsCount}' complete.");
                await sender.DisposeAsync();
            }
        }

        public async Task PeekSampleMessagesAsync(ServiceBusReceiver receiver)
        {
            if (peekedMsgs == null) { peekedMsgs = new List<ServiceBusReceivedMessage>(); }

            Console.WriteLine("\nPeeking sample messages ...");
            peekedMsgs.AddRange(await receiver.PeekMessagesAsync(sampleMsgsCount));
            Console.WriteLine("\nPeeking sample messages complete.\n");
        }

        public async Task PeekDeadLetterSampleMessagesAsync(ServiceBusReceiver receiver)
        {
            if (peekedDeadLetterMsgs == null) { peekedDeadLetterMsgs = new List<ServiceBusReceivedMessage>(); }

            Console.WriteLine("Peeking dead letter messages...");
            peekedDeadLetterMsgs.AddRange(await receiver.PeekMessagesAsync(sampleMsgsCount));
        }

        public async Task ReceiveSampleMessagesAsync(ServiceBusReceiver receiver)
        {
            if (receivedMsgs == null) { receivedMsgs = new List<ServiceBusReceivedMessage>(); }

            Console.WriteLine("Receiving messages...");
            receivedMsgs.AddRange(await receiver.ReceiveMessagesAsync(sampleMsgsCount));
        }

        public async Task ReleaseLocksForReceiveedMsgsAsync(ServiceBusReceiver receiver)
        {
            Console.WriteLine("\nExplicit lock release using 'Abandon message cmd' for received messages...");

            foreach (var receivedMessage in receivedMsgs)
            {
                try
                {
                    await receiver.AbandonMessageAsync(receivedMessage);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Lock release error: {ex.Message}");
                }
            }

            Console.WriteLine("\nExplicit lock release complete");
        }

        public async Task DeferMessagesAsync(ServiceBusReceiver receiver)
        {
            if (receivedMsgs.Any())
            {
                Console.WriteLine("\nSetting defered state for messages...");
                foreach (var msg in receivedMsgs)
                {
                    await receiver.DeferMessageAsync(msg);
                }
                Console.WriteLine("Updated messages to deferred state.\n");
            }
        }

        public async Task CompletedDeferredMsgsAsync(ServiceBusReceiver receiver)
        {
            if (peekedDeadLetterMsgs.Any())
            {
                Console.WriteLine("\nCompleting deferred state for messages...");
                foreach (var msg in peekedDeadLetterMsgs)
                {
                    ServiceBusReceivedMessage serviceBusReceivedMessage = await receiver.ReceiveDeferredMessageAsync(msg.SequenceNumber);
                    await receiver.CompleteMessageAsync(serviceBusReceivedMessage);
                }
                Console.WriteLine("Completed deferred msgs.\n");
            }
        }

        public async Task ReadDeferMessagesAsync(ServiceBusReceiver receiver)
        {
            if (peekedMsgs == null)
            {
                peekedMsgs = new List<ServiceBusReceivedMessage>();
            }
            else
            {
                peekedMsgs.Clear();
            }

            Console.WriteLine("\nReading deferred messages using peek...\n");
            await PeekSampleMessagesAsync(receiver);

            PrintPeekedMsgs();
            Console.WriteLine("\nReading deferred messages using peek complete.\n");
        }

        public void ClearAllCollections()
        {
            Console.WriteLine("\nClearing all local collections and resets to null.\n");
            if (peekedMsgs != null)
            {
                peekedMsgs.Clear();
                peekedMsgs = null;
            }

            if (receivedMsgs != null)
            {
                receivedMsgs.Clear();
                receivedMsgs = null;
            }

            if (peekedDeadLetterMsgs != null)
            {
                peekedDeadLetterMsgs.Clear();
                peekedDeadLetterMsgs = null;
            }
        }

        public async Task CleanUpQueueAsync()
        {
            Console.WriteLine($"\nCleaning up queue: '{queueName}' before executing the scenario.");
            await DeleteQueueAsync();
            await CreateQueueIfNotExistsAsync();
            await RefreshQueueAsync();
            Console.WriteLine($"\nCompleted queue: '{queueName}' clean up.\n");
        }

        public void PrintState()
        {
            int peekedMsgsCount = 0;
            int receivedMsgsCount = 0;
            int peekedDeadLetterMsgsCount = 0;

            if (peekedMsgs != null && peekedMsgs.Any())
            {
                peekedMsgsCount = peekedMsgs.Count();
            }

            if (receivedMsgs != null && receivedMsgs.Any())
            {
                receivedMsgsCount = receivedMsgs.Count();
            }

            if (peekedDeadLetterMsgs != null && peekedDeadLetterMsgs.Any())
            {
                peekedDeadLetterMsgsCount = peekedDeadLetterMsgs.Count();
            }
            Console.WriteLine($"\n**** Queue: '{queueName}' - State ****");
            Console.WriteLine($"Peeked msgs count: {peekedMsgsCount}");
            Console.WriteLine($"Received msgs count: {receivedMsgsCount}");
            Console.WriteLine($"Peeked Dead letter msgs count: {peekedDeadLetterMsgsCount}");
            Console.WriteLine($"**** End ****\n");
        }

        public async Task RefreshQueueAsync()
        {
            var client = GetServiceBusAdministrationClient();
            Console.WriteLine($"\nRefreshing queue: '{queueName}' to make sure it is ready for consumption...");

            bool isAvailable = false;
            while (!isAvailable)
            {
                try
                {
                    queueProperties = await client.GetQueueAsync(queueName);
                    Console.WriteLine($"Queue: '{queueName}' is ready for consumption.\n");
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"\nServer might take some time to provision this entity, wait until it is ready. Exception:{ex.Message}\n");
                }
                System.Threading.Thread.Sleep(TimeSpan.FromSeconds(2));
            }
        }

        public ServiceBusClient GetServiceBusClient()
        {
            if (serviceBusClient == null)
            {
                serviceBusClient = new ServiceBusClient(sbConnectionStr);
            }

            return serviceBusClient;
        }

        public void PrintPeekedMsgs()
        {
            Console.WriteLine("\n Printing peeked messages...");
            PrintReceivedMsgs(peekedMsgs);
            Console.WriteLine("\n Printing peeked messages complete.\n");
        }

        public void PrintReceivedMsgs()
        {
            Console.WriteLine("\n Printing received messages...");
            PrintReceivedMsgs(receivedMsgs);
            Console.WriteLine("\n Printing received messages complete.\n");
        }

        public void PrintDeadLetterMsgs()
        {
            Console.WriteLine("\n Printing dead letter messages...");
            PrintReceivedMsgs(peekedDeadLetterMsgs, true);
            Console.WriteLine("\n Printing dead letter messages complete.");
        }

        private void PrintReceivedMsgs(List<ServiceBusReceivedMessage> msgs, bool isDeadLetter = false)
        {
            foreach (var msg in msgs)
            {
                if (isDeadLetter)
                {
                    Console.WriteLine($"\n Msg: Id:{msg.MessageId} - State: {msg.State} - Sequence Number: {msg.SequenceNumber} - EnqueuedSequenceNumber: {msg.EnqueuedSequenceNumber} - Enqueued Time: {msg.EnqueuedTime} - Dead Letter details: {msg.DeadLetterReason} - {msg.DeadLetterSource} - {msg.DeadLetterErrorDescription}");
                }
                else
                {
                    Console.WriteLine($"\n Msg: Id:{msg.MessageId} - State: {msg.State} - Sequence Number: {msg.SequenceNumber} - EnqueuedSequenceNumber: {msg.EnqueuedSequenceNumber} - Enqueued Time: {msg.EnqueuedTime}");
                }
            }
        }

        private ServiceBusAdministrationClient GetServiceBusAdministrationClient()
        {
            if (serviceBusAdministrationClient == null)
            {
                serviceBusAdministrationClient = new ServiceBusAdministrationClient(sbConnectionStr);
            }

            return serviceBusAdministrationClient;
        }

        private async Task<bool> CheckIfQueueExitsAsync()
        {
            var client = GetServiceBusAdministrationClient();

            try
            {
                Console.WriteLine($"\nChecking if queue: '{queueName}' exists?");
                queueProperties = await client.GetQueueAsync(queueName);
                Console.WriteLine($"Queue: '{queueName}' found.\n");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nQueue: '{queueName}' doesn't exist.Exception: {ex.Message}");
            }

            return false;
        }
    }
}