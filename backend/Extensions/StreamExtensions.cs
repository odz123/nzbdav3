using System.Buffers;
using System.Runtime.CompilerServices;
using NzbWebDAV.Streams;

namespace NzbWebDAV.Extensions;

public static class StreamExtensions
{
    private const int DrainBufferSize = 8192; // 8KB for efficient draining

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Stream LimitLength(this Stream stream, long length)
    {
        return new LimitedLengthStream(stream, length);
    }

    /// <summary>
    /// Drains the stream to completion using pooled buffers to avoid allocations.
    /// </summary>
    public static void Drain(this Stream stream)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(DrainBufferSize);
        try
        {
            while (stream.Read(buffer, 0, buffer.Length) > 0) ;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Drains the stream asynchronously using pooled buffers.
    /// </summary>
    public static async ValueTask DrainAsync(this Stream stream, CancellationToken ct = default)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(DrainBufferSize);
        try
        {
            while (await stream.ReadAsync(buffer.AsMemory(), ct).ConfigureAwait(false) > 0) ;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Stream OnDispose(this Stream stream, Action onDispose)
    {
        return new DisposableCallbackStream(stream, onDispose, async () => onDispose?.Invoke());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Stream OnDisposeAsync(this Stream stream, Func<ValueTask> onDisposeAsync)
    {
        return new DisposableCallbackStream(stream, onDisposeAsync: onDisposeAsync);
    }
}