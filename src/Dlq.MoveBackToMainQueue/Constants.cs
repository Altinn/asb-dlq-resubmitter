namespace Cjoergensen.Azure.ServiceBus.Tools.Dlq.MoveBackToMainQueue;

public static class Constants
{
    public const int DefaultMaxConcurrency = 8;
    public const string DefaultIdentifier = "DLQMove";
    public const int MaxAllowedConcurrency = 32;
    public const int DefaultMaxReplayAttempts = 3;
    public const int MaxAllowedReplayAttempts = 10;
    public const string ReplayCountPropertyName = "dlq-replay-count";
}
