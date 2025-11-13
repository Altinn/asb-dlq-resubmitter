using System.Globalization;
using System.Runtime.CompilerServices;
using Azure.Messaging.ServiceBus;
using FluentResults;

namespace Cjoergensen.Azure.ServiceBus.Tools.Dlq.MoveBackToMainQueue;

public class DeadLetterMessageMover(string connectionString, string queueName, string identifier = "", int maxReplayAttempts = Constants.DefaultMaxReplayAttempts)
{
    private readonly ServiceBusClient client = new(connectionString);
    private readonly int maxReplayAttempts = maxReplayAttempts;
    private readonly HashSet<string> exhaustedMessageIds = new();

    public async IAsyncEnumerable<FluentResults.Result> MoveMessagesAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await using var sender = client.CreateSender(queueName, new ServiceBusSenderOptions
            {
                Identifier = identifier
            });

            await using var receiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions
            {
                SubQueue = SubQueue.DeadLetter,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                Identifier = identifier
            });
            
            await foreach (var msg in receiver.ReceiveMessagesAsync(cancellationToken))
            {
                if(msg == null)
                {
                    yield break; // No more messages to process
                }

                var currentReplayCount = GetReplayCount(msg);
                if (currentReplayCount >= maxReplayAttempts)
                {
                    var messageIdentifier = GetMessageIdentifier(msg);
                    if (exhaustedMessageIds.Add(messageIdentifier))
                    {
                        yield return Result.Fail($"Replay limit reached for message: {messageIdentifier} (attempts: {currentReplayCount}). Message left in DLQ.");
                    }
                    
                    await receiver.AbandonMessageAsync(msg, cancellationToken: cancellationToken);
                    continue;
                }
                
                var result = Result.Ok();
                try
                {
                    var clonedMessage = MessageCloner.Clone(msg);
                    clonedMessage.ApplicationProperties[Constants.ReplayCountPropertyName] = currentReplayCount + 1;
                    
                    await sender.SendMessageAsync(clonedMessage, cancellationToken);
                    await receiver.CompleteMessageAsync(msg, cancellationToken);
                }
                catch (Exception ex)
                {
                    await receiver.AbandonMessageAsync(msg, cancellationToken: cancellationToken);
                    result = Result.Fail($"Failed to move message {msg.MessageId}: {ex.Message}");
                }

                yield return result;
            }
        }
    }

    private static int GetReplayCount(ServiceBusReceivedMessage message)
    {
        if (message.ApplicationProperties.TryGetValue(Constants.ReplayCountPropertyName, out var value))
        {
            return value switch
            {
                int i => i,
                long l => (int)l,
                string s when int.TryParse(s, out var parsed) => parsed,
                _ => 0
            };
        }

        return 0;
    }

    private static string GetMessageIdentifier(ServiceBusReceivedMessage message)
    {
        return !string.IsNullOrWhiteSpace(message.MessageId)
            ? message.MessageId
            : message.SequenceNumber.ToString(CultureInfo.InvariantCulture);
    }
}
