using System.Runtime.CompilerServices;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Extensions;
using NzbWebDAV.Models;
using NzbWebDAV.Utils;

namespace NzbWebDAV.Streams;

public class NzbFileStream(
    string[] fileSegmentIds,
    long fileSize,
    INntpClient client,
    int concurrentConnections
) : Stream
{
    private long _position;
    private CombinedStream? _innerStream;
    private bool _disposed;

    // Cache segment metadata for fast seeking - avoids network round-trips
    private readonly LongRange[] _segmentRangeCache = new LongRange[fileSegmentIds.Length];
    private readonly bool[] _segmentCached = new bool[fileSegmentIds.Length];

    public override void Flush()
    {
        _innerStream?.Flush();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return ReadAsync(buffer, offset, count).GetAwaiter().GetResult();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        _innerStream ??= await GetFileStream(_position, cancellationToken).ConfigureAwait(false);
        var read = await _innerStream.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false);
        _position += read;
        return read;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        _innerStream ??= await GetFileStream(_position, cancellationToken).ConfigureAwait(false);
        var read = await _innerStream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        _position += read;
        return read;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        var absoluteOffset = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => fileSize + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };

        if (_position == absoluteOffset) return _position;
        _position = absoluteOffset;
        _innerStream?.Dispose();
        _innerStream = null;
        return _position;
    }

    public override void SetLength(long value)
    {
        throw new InvalidOperationException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new InvalidOperationException();
    }

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length => fileSize;

    public override long Position
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _position;
        set => Seek(value, SeekOrigin.Begin);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async ValueTask<LongRange> GetSegmentRangeAsync(int index, CancellationToken ct)
    {
        // Fast path: check local cache first
        if (_segmentCached[index])
        {
            return _segmentRangeCache[index];
        }

        // Slow path: fetch from client (which has its own cache)
        var header = await client.GetSegmentYencHeaderAsync(fileSegmentIds[index], ct).ConfigureAwait(false);
        var range = new LongRange(header.PartOffset, header.PartOffset + header.PartSize);

        // Cache locally for this stream instance
        _segmentRangeCache[index] = range;
        _segmentCached[index] = true;

        return range;
    }

    private async Task<InterpolationSearch.Result> SeekSegment(long byteOffset, CancellationToken ct)
    {
        // Single segment optimization - no search needed
        if (fileSegmentIds.Length == 1)
        {
            return new InterpolationSearch.Result(0, new LongRange(0, fileSize));
        }

        return await InterpolationSearch.Find(
            byteOffset,
            new LongRange(0, fileSegmentIds.Length),
            new LongRange(0, fileSize),
            async (guess) => await GetSegmentRangeAsync(guess, ct).ConfigureAwait(false),
            ct
        ).ConfigureAwait(false);
    }

    private async Task<CombinedStream> GetFileStream(long rangeStart, CancellationToken cancellationToken)
    {
        if (rangeStart == 0) return GetCombinedStream(0, cancellationToken);
        var foundSegment = await SeekSegment(rangeStart, cancellationToken).ConfigureAwait(false);
        var stream = GetCombinedStream(foundSegment.FoundIndex, cancellationToken);
        var bytesToDiscard = rangeStart - foundSegment.FoundByteRange.StartInclusive;
        if (bytesToDiscard > 0)
        {
            await stream.DiscardBytesAsync(bytesToDiscard).ConfigureAwait(false);
        }
        return stream;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private CombinedStream GetCombinedStream(int firstSegmentIndex, CancellationToken ct)
    {
        return new CombinedStream(
            fileSegmentIds[firstSegmentIndex..]
                .Select(async x => (Stream)await client.GetSegmentStreamAsync(x, false, ct).ConfigureAwait(false))
                .WithConcurrency(concurrentConnections)
        );
    }

    protected override void Dispose(bool disposing)
    {
        if (_disposed) return;
        _innerStream?.Dispose();
        _disposed = true;
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        if (_innerStream != null) await _innerStream.DisposeAsync().ConfigureAwait(false);
        _disposed = true;
        GC.SuppressFinalize(this);
    }
}