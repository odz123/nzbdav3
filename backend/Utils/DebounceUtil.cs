using System.Runtime.CompilerServices;

namespace NzbWebDAV.Utils;

public static class DebounceUtil
{
    /// <summary>
    /// Creates a lock-free debounce function using atomic operations.
    /// Much faster under contention than lock-based approach.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Action<Action> CreateDebounce(TimeSpan timespan)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(timespan, TimeSpan.Zero);
        var timespanTicks = timespan.Ticks;
        long lastInvocationTicks = 0;

        return actionToMaybeInvoke =>
        {
            var nowTicks = DateTime.UtcNow.Ticks;
            var lastTicks = Volatile.Read(ref lastInvocationTicks);

            // Fast path: check if enough time has passed
            if (nowTicks - lastTicks < timespanTicks) return;

            // Try to atomically update the last invocation time
            // Only proceed if we successfully updated from the old value
            if (Interlocked.CompareExchange(ref lastInvocationTicks, nowTicks, lastTicks) == lastTicks)
            {
                actionToMaybeInvoke?.Invoke();
            }
        };
    }

    /// <summary>
    /// Creates a lock-free run-once function.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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