# Important!

- These samples are for educational purpose, valid against premium namespace(mostly works on standard namespace wherever the feature set is same) and have redundant code for reading purpose. 

# Scenarios:

- Q_Send_ReceiveAsync: Sends some sample messages and receives it.

- Q_Send_AutoMsgExpiry_DeadLetterAsync: Sends some sample messages with TTL, waits until the messages are moved to dead letter queue and then prints dead letter messages

- Q_Send_DeferAsync: Sends some sample messages and changes state of the messages to "Defer" state by receiving it. Please note that once the messages are in defer state it won't apply TTL and keeps messages until it gets processed explicitly.

- Q_Send_AutoMsgExpiry_DeadLetter_SetDeferStateAsync: Sends some sample messages with TTL, waits until the messages are moved to dead letter queue. Once messages are available in dead letter queue, it change state of the message to 'defer' and prints messages.

- Q_Send_AutoMsgExpiry_DeadLetter_SetDeferState_CompleteAsync:  Sends some sample messages with TTL, waits until the messages are moved to dead letter queue. Once messages are available in dead letter queue, it change state of the message to 'defer' and then completes it.

- DeleteAllScenarioQueuesAsync:  Deletes queues created by this sample.

### Code snippets used in this sample:

- https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/servicebus/Azure.Messaging.ServiceBus/samples/Sample01_HelloWorld.md

- https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/servicebus/Azure.Messaging.ServiceBus/samples/Sample07_CrudOperations.md

- https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/servicebus/Azure.Messaging.ServiceBus/samples/Sample02_MessageSettlement.md