namespace NzbWebDAV.Utils;

public static class DebounceUtil
{
    public static Action<Action> CreateDebounce(TimeSpan timespan)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(timespan, TimeSpan.Zero);
        var synchronizationLock = new object();
        DateTime lastInvocationTime = default;
        return actionToMaybeInvoke =>
        {
            var now = DateTime.Now;
            bool shouldInvoke;
            lock (synchronizationLock)
            {
                if (now - lastInvocationTime >= timespan)
                {
                    lastInvocationTime = now;
                    shouldInvoke = true;
                }
                else
                {
                    shouldInvoke = false;
                }
            }

            if (shouldInvoke)
                actionToMaybeInvoke?.Invoke();
        };
    }

    public static Action<Action> RunOnlyOnce()
    {
        var hasRun = 0; // 0 = false, 1 = true (using int for Interlocked)
        return actionToMaybeInvoke =>
        {
            if (Interlocked.CompareExchange(ref hasRun, 1, 0) == 0)
            {
                actionToMaybeInvoke?.Invoke();
            }
        };
    }
}