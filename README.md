# RecoverDeadLetterQueuesGeneric

This was a emergencial project created ASAP during the first crisis on dawn with our clients due the wrong firewall rules on client side.
So, this app reprocess ou abandon messages on queues or topics on Service Bus.
Go to the Program.cs and choose the option that you would to use and comment the another ones.

At PurgeMessagesFromSubscription() and RecoverTopic() you need to create the Dictionary with topics and subscriptions.
I know, this is not the best option but you need to comment the topics and subs that you won't use.

Press play and be happy.
