using System.Buffers;
using System.Runtime.CompilerServices;

namespace NzbWebDAV.Streams;

/// <summary>
/// High-performance stream that combines multiple streams into a single sequential stream.
/// Features:
/// - Pre-fetching of next stream to reduce latency between segments
/// - Pooled buffer for discard operations
/// - Zero-allocation read path on common cases
/// </summary>
public sealed class CombinedStream : Stream
{
    private const int DiscardBufferSize = 65536; // 64KB buffer for maximum throughput

    private readonly IEnumerator<Task<Stream>> _streams;
    private Stream? _currentStream;
    private Task<Stream>? _nextStreamTask; // Pre-fetch next stream
    private long _position;
    private bool _isDisposed;
    private bool _exhausted;
    private bool _hasMoreStreams = true;

    public CombinedStream(IEnumerable<Task<Stream>> streams)
    {
        _streams = streams.GetEnumerator();
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _position;
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return ReadAsync(buffer, offset, count).GetAwaiter().GetResult();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return ReadAsyncCore(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return ReadAsyncCore(buffer, cancellationToken);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async ValueTask<int> ReadAsyncCore(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        // Fast path: empty buffer or exhausted stream
        if (buffer.Length == 0 || _exhausted) return 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            // Initialize current stream if needed
            if (_currentStream == null)
            {
                if (!await TryMoveToNextStreamAsync().ConfigureAwait(false))
                {
                    return 0;
                }
            }

            // Read from current stream
            var readCount = await _currentStream!.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (readCount > 0)
            {
                _position += readCount;
                return readCount;
            }

            // Current stream exhausted - dispose and move to next
            await _currentStream.DisposeAsync().ConfigureAwait(false);
            _currentStream = null;

            if (!await TryMoveToNextStreamAsync().ConfigureAwait(false))
            {
                return 0;
            }
        }

        return 0;
    }

    /// <summary>
    /// Attempts to move to the next stream, using pre-fetched stream if available.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async ValueTask<bool> TryMoveToNextStreamAsync()
    {
        // Use pre-fetched stream if available
        if (_nextStreamTask != null)
        {
            _currentStream = await _nextStreamTask.ConfigureAwait(false);
            _nextStreamTask = null;
            // Pre-fetch next stream
            PrefetchNextStream();
            return true;
        }

        // No pre-fetched stream, try to get next from enumerator
        if (!_hasMoreStreams)
        {
            _exhausted = true;
            return false;
        }

        if (!_streams.MoveNext())
        {
            _hasMoreStreams = false;
            _exhausted = true;
            return false;
        }

        _currentStream = await _streams.Current.ConfigureAwait(false);
        // Pre-fetch next stream for better latency
        PrefetchNextStream();
        return true;
    }

    /// <summary>
    /// Pre-fetches the next stream to reduce latency when transitioning between segments.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PrefetchNextStream()
    {
        if (_hasMoreStreams && _nextStreamTask == null)
        {
            if (_streams.MoveNext())
            {
                _nextStreamTask = _streams.Current;
            }
            else
            {
                _hasMoreStreams = false;
            }
        }
    }

    /// <summary>
    /// Efficiently discards bytes from the stream using pooled buffers.
    /// </summary>
    public async Task DiscardBytesAsync(long count)
    {
        if (count <= 0 || _exhausted) return;

        var remaining = count;
        var buffer = ArrayPool<byte>.Shared.Rent(DiscardBufferSize);
        try
        {
            while (remaining > 0)
            {
                var toRead = (int)Math.Min(remaining, buffer.Length);
                var read = await ReadAsyncCore(buffer.AsMemory(0, toRead), CancellationToken.None).ConfigureAwait(false);
                if (read == 0) break;
                remaining -= read;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public override void Flush() => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (_isDisposed) return;
        if (disposing)
        {
            _streams.Dispose();
            _currentStream?.Dispose();
            // Note: _nextStreamTask is intentionally not disposed here
            // as the task itself will complete and the stream will be collected
        }
        _isDisposed = true;
        _exhausted = true;
    }

    public override async ValueTask DisposeAsync()
    {
        if (_isDisposed) return;

        if (_currentStream != null)
        {
            await _currentStream.DisposeAsync().ConfigureAwait(false);
        }

        // Dispose pre-fetched stream if it exists
        if (_nextStreamTask != null)
        {
            try
            {
                var nextStream = await _nextStreamTask.ConfigureAwait(false);
                await nextStream.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Ignore errors when disposing pre-fetched stream
            }
        }

        _streams.Dispose();
        _isDisposed = true;
        _exhausted = true;
        GC.SuppressFinalize(this);
    }
}