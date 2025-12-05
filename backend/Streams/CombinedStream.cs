using System.Buffers;

namespace NzbWebDAV.Streams;

public class CombinedStream(IEnumerable<Task<Stream>> streams) : Stream
{
    private const int DiscardBufferSize = 65536; // 64KB buffer for maximum throughput
    private readonly IEnumerator<Task<Stream>> _streams = streams.GetEnumerator();
    private Stream? _currentStream;
    private long _position;
    private bool _isDisposed;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return ReadAsync(buffer, offset, count).GetAwaiter().GetResult();
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return ReadAsyncCore(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return ReadAsyncCore(buffer, cancellationToken);
    }

    private async ValueTask<int> ReadAsyncCore(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        if (buffer.Length == 0) return 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            // If we haven't read the first stream, read it.
            if (_currentStream == null)
            {
                if (!_streams.MoveNext()) return 0;
                _currentStream = await _streams.Current.ConfigureAwait(false);
            }

            // read from our current stream
            var readCount = await _currentStream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            _position += readCount;
            if (readCount > 0) return readCount;

            // If we couldn't read anything from our current stream,
            // it's time to advance to the next stream.
            await _currentStream.DisposeAsync().ConfigureAwait(false);
            if (!_streams.MoveNext()) return 0;
            _currentStream = await _streams.Current.ConfigureAwait(false);
        }

        return 0;
    }

    public async Task DiscardBytesAsync(long count)
    {
        if (count <= 0) return;
        var remaining = count;
        var buffer = ArrayPool<byte>.Shared.Rent(DiscardBufferSize);
        try
        {
            while (remaining > 0)
            {
                var toRead = (int)Math.Min(remaining, buffer.Length);
                var read = await ReadAsync(buffer, 0, toRead).ConfigureAwait(false);
                if (read == 0) break;
                remaining -= read;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public override void Flush()
    {
        throw new NotSupportedException();
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    protected override void Dispose(bool disposing)
    {
        if (_isDisposed) return;
        if (!disposing) return;
        _streams.Dispose();
        _currentStream?.Dispose();
        _isDisposed = true;
    }

    public override async ValueTask DisposeAsync()
    {
        if (_isDisposed) return;
        if (_currentStream != null) await _currentStream.DisposeAsync().ConfigureAwait(false);
        _streams.Dispose();
        _isDisposed = true;
        GC.SuppressFinalize(this);
    }
}