// ReSharper disable InconsistentNaming

using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace NzbWebDAV.Extensions;

public static class IEnumerableTaskExtensions
{
    /// <summary>
    /// Executes tasks with specified concurrency and enumerates results as they come in.
    /// Uses a queue-based approach for predictable ordering and better time-to-first-byte.
    /// </summary>
    /// <param name="tasks">The tasks to execute</param>
    /// <param name="concurrency">The max concurrency</param>
    /// <typeparam name="T">The resulting type of each task</typeparam>
    /// <returns>An IEnumerable that yields tasks in order they were queued</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Task<T>> WithConcurrency<T>
    (
        this IEnumerable<Task<T>> tasks,
        int concurrency
    ) where T : IDisposable
    {
        if (concurrency < 1)
            throw new ArgumentException("concurrency must be greater than zero.");

        // Fast path for single concurrency
        if (concurrency == 1)
        {
            foreach (var task in tasks) yield return task;
            yield break;
        }

        // Pre-allocate queue with expected capacity to avoid resizing
        var runningTasks = new Queue<Task<T>>(concurrency);
        var isFirst = true;

        try
        {
            foreach (var task in tasks)
            {
                if (isFirst)
                {
                    // help with time-to-first-byte
                    yield return task;
                    isFirst = false;
                    continue;
                }

                runningTasks.Enqueue(task);
                if (runningTasks.Count < concurrency) continue;
                yield return runningTasks.Dequeue();
            }

            while (runningTasks.Count > 0)
                yield return runningTasks.Dequeue();
        }
        finally
        {
            // Cleanup remaining tasks without allocating closures
            while (runningTasks.TryDequeue(out var remainingTask))
            {
                DisposeOnCompletion(remainingTask);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void DisposeOnCompletion<T>(Task<T> task) where T : IDisposable
    {
        if (task.IsCompleted)
        {
            if (task.IsCompletedSuccessfully)
                task.Result.Dispose();
        }
        else
        {
            task.ContinueWith(static t =>
            {
                if (t.IsCompletedSuccessfully)
                    t.Result.Dispose();
            }, TaskContinuationOptions.ExecuteSynchronously);
        }
    }

    /// <summary>
    /// Executes tasks with specified concurrency and yields results as they complete.
    /// Uses a Channel-based approach for O(1) completion notification instead of O(n) Task.WhenAny.
    /// </summary>
    public static async IAsyncEnumerable<T> WithConcurrencyAsync<T>
    (
        this IEnumerable<Task<T>> tasks,
        int concurrency
    )
    {
        if (concurrency < 1)
            throw new ArgumentException("concurrency must be greater than zero.");

        // Use an unbounded channel with optimized settings
        var channel = Channel.CreateUnbounded<Task<T>>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = true // Reduces context switches
        });

        var activeCount = 0;
        var allEnqueued = false;

        using var enumerator = tasks.GetEnumerator();

        // Start initial batch up to concurrency limit
        while (activeCount < concurrency && enumerator.MoveNext())
        {
            var task = enumerator.Current;
            activeCount++;
            _ = CompleteAndNotifyAsync(task, channel.Writer);
        }

        // If no tasks, we're done
        if (activeCount == 0)
        {
            yield break;
        }

        // Process completions
        while (activeCount > 0)
        {
            // Wait for a task to complete - O(1) read from channel
            var completedTask = await channel.Reader.ReadAsync().ConfigureAwait(false);
            activeCount--;

            // Try to start another task if available
            if (!allEnqueued && enumerator.MoveNext())
            {
                var nextTask = enumerator.Current;
                activeCount++;
                _ = CompleteAndNotifyAsync(nextTask, channel.Writer);
            }
            else
            {
                allEnqueued = true;
            }

            // Yield the result (will throw if task faulted)
            yield return await completedTask.ConfigureAwait(false);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static async Task CompleteAndNotifyAsync<T>(Task<T> task, ChannelWriter<Task<T>> writer)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch
        {
            // Exception will be observed when the task is awaited in the main loop
        }
        finally
        {
            // Notify that this task completed - O(1) write
            writer.TryWrite(task);
        }
    }
}