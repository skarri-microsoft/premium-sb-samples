
using System.Threading.Tasks;

namespace premium_sb_samples
{
    internal class Program
    {
        // Go to azure portal -> open your Service bus namespace resource -> click on "Shared access policies" under "Settings" -> Click on policy item -> It will show both primary and secondary connection strings
        private const string connectionString = "your connection string";

        private static async Task Main(string[] args)
        {
            await Run();
        }

        private static async Task Run()
        {
            // Please go through README.md before trying these scenarios.
            //await QueueScenarios.Q_Send_ReceiveAsync(connectionString);
            //await QueueScenarios.Q_Send_AutoMsgExpiry_DeadLetterAsync(connectionString);
            //await QueueScenarios.Q_Send_DeferAsync(connectionString);
            //await QueueScenarios.Q_Send_AutoMsgExpiry_DeadLetter_SetDeferStateAsync(connectionString);
            //await QueueScenarios.Q_Send_AutoMsgExpiry_DeadLetter_SetDeferState_CompleteAsync(connectionString);
            //await QueueScenarios.DeleteAllScenarioQueuesAsync(connectionString);
            await QueueScenarios.Q_Large_Msg_Send_ReceiveAsync(connectionString);
        }
    }
}