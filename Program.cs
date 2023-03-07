
using System.Threading.Tasks;

namespace premium_sb_samples
{
    internal class Program
    {
        // Go to azure portal -> open your Service bus namespace resource -> click on "Shared access policies" under "Settings" -> Click on policy item -> It will show both primary and secondary connection strings
        private const string connectionString = "";

        private static async Task Main(string[] args)
        {
            await Run();
        }

        private static async Task Run()
        {
            // Please go through README.md before trying these scenarios.

            //await QueueScenarios.Q_Send_ReceiveAsync(connectionString);

            //await QueueScenarios.Q_Send_ScheduleAsync(connectionString);

            //await QueueScenarios.Q_Send_Dead_Letter_A_Msg_Async(connectionString);

            //await QueueScenarios.Q_Receive_Dead_Letter_Msgs_Async(connectionString);

            //await QueueScenarios.Q_Send_AutoMsgExpiry_DeadLetterAsync(connectionString);

            // Create a queue with name 'q_send_defer' and sends some sample messages and sets deferred state.
            // Note: if there is an existing queue, deletes it first
            //await QueueScenarios.Q_Send_DeferAsync(connectionString);

            // Note(s):
            // #1 This scenario throws the following error when ReceiveDeferredMessageAsync is invoked
            // Message = Failed to lock one or more specified messages. Either the message is already locked or does not exist.
            // If the lock expiry is high like 5mins and not releases the lock
            //
            // #2 This method throws and moves a message to DeadLetterQueue with State=0 (active)
            // when a active message with deferred state expired
            // For example if a message has 1min ttl and expired at the time calling ReceiveDeferredMessageAsync method
            // It throws the above error and moves it to dead letter queue automatically.
            // 
            // #3 To run this scenario please make sure 'q_send_defer' has some messages with state 'differed' in the active queue. 
            //await QueueScenarios.Q_Dead_Letter_Deferred_Msgs_Async(connectionString);

            // Note(s):
            // #1 To run this scenario please make sure 'q_send_defer' has some messages with state 'differed' in the dead letter queue. 
            //await QueueScenarios.Q_Set_Defer_State_For_DeadLetterQueueAsync(connectionString);

            // To run below scenario make sure the passing queue has dead letter messages in deferred state.
            await QueueScenarios.Q_Complete_DeadLetter_Deferrred_Msgs_Async(connectionString, Constants.SampleQueueNames.q_send_defer, 10);

            //await QueueScenarios.Q_Send_AutoMsgExpiry_DeadLetter_SetDeferStateAsync(connectionString);

            //await QueueScenarios.Q_Send_AutoMsgExpiry_DeadLetter_SetDeferState_CompleteAsync(connectionString);

            //await QueueScenarios.DeleteAllScenarioQueuesAsync(connectionString);

            //await QueueScenarios.Q_Large_Msg_Send_ReceiveAsync(connectionString);
        }
    }
}