// ReSharper disable InconsistentNaming

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
    public static IEnumerable<Task<T>> WithConcurrency<T>
    (
        this IEnumerable<Task<T>> tasks,
        int concurrency
    ) where T : IDisposable
    {
        if (concurrency < 1)
            throw new ArgumentException("concurrency must be greater than zero.");

        if (concurrency == 1)
        {
            foreach (var task in tasks) yield return task;
            yield break;
        }

        var isFirst = true;
        var runningTasks = new Queue<Task<T>>();
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
            while (runningTasks.Count > 0)
            {
                runningTasks.Dequeue().ContinueWith(x =>
                {
                    if (x.Status == TaskStatus.RanToCompletion)
                        x.Result.Dispose();
                });
            }
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

        // Use an unbounded channel for completion notifications - O(1) writes
        var channel = Channel.CreateUnbounded<Task<T>>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        var activeCount = 0;
        var allEnqueued = false;

        using var enumerator = tasks.GetEnumerator();

        // Start initial batch up to concurrency limit
        while (activeCount < concurrency && enumerator.MoveNext())
        {
            var task = enumerator.Current;
            activeCount++;
            _ = CompleteAndNotify(task, channel.Writer);
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
                _ = CompleteAndNotify(nextTask, channel.Writer);
            }
            else
            {
                allEnqueued = true;
            }

            // Yield the result (will throw if task faulted)
            yield return await completedTask.ConfigureAwait(false);
        }

        static async Task CompleteAndNotify(Task<T> task, ChannelWriter<Task<T>> writer)
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
}