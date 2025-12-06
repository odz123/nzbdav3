using System.Runtime.CompilerServices;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Extensions;
using NzbWebDAV.Models;
using NzbWebDAV.Utils;

namespace NzbWebDAV.Streams;

/// <summary>
/// High-performance seekable stream for NZB file content.
/// Uses interpolation search for fast seeking and local caching to minimize network round-trips.
/// </summary>
public sealed class NzbFileStream : Stream
{
    private readonly string[] _fileSegmentIds;
    private readonly long _fileSize;
    private readonly INntpClient _client;
    private readonly int _concurrentConnections;

    private long _position;
    private CombinedStream? _innerStream;
    private bool _disposed;

    // Cache segment metadata for fast seeking - avoids network round-trips
    // Using a single array with nullable structs for better cache locality
    private readonly LongRange?[] _segmentRangeCache;

    // Pre-computed values for single-segment optimization
    private readonly bool _isSingleSegment;
    private readonly LongRange _singleSegmentRange;

    public NzbFileStream(
        string[] fileSegmentIds,
        long fileSize,
        INntpClient client,
        int concurrentConnections)
    {
        _fileSegmentIds = fileSegmentIds;
        _fileSize = fileSize;
        _client = client;
        _concurrentConnections = concurrentConnections;

        // Single segment optimization - pre-compute the range
        _isSingleSegment = fileSegmentIds.Length == 1;
        _singleSegmentRange = _isSingleSegment ? new LongRange(0, fileSize) : default;

        // Only allocate cache if we have multiple segments
        _segmentRangeCache = _isSingleSegment ? Array.Empty<LongRange?>() : new LongRange?[fileSegmentIds.Length];
    }

    public override void Flush()
    {
        _innerStream?.Flush();
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
        _innerStream ??= await GetFileStreamAsync(_position, cancellationToken).ConfigureAwait(false);
        var read = await _innerStream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        _position += read;
        return read;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override long Seek(long offset, SeekOrigin origin)
    {
        var absoluteOffset = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => _fileSize + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };

        if (_position == absoluteOffset) return _position;
        _position = absoluteOffset;

        // Dispose current stream and clear reference
        var streamToDispose = _innerStream;
        _innerStream = null;
        streamToDispose?.Dispose();

        return _position;
    }

    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length => _fileSize;

    public override long Position
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _position;
        set => Seek(value, SeekOrigin.Begin);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask<LongRange> GetSegmentRangeAsync(int index, CancellationToken ct)
    {
        // Fast path: check local cache first (single array lookup)
        var cached = _segmentRangeCache[index];
        if (cached.HasValue)
        {
            return new ValueTask<LongRange>(cached.Value);
        }

        // Slow path: fetch from client
        return GetSegmentRangeSlowAsync(index, ct);
    }

    private async ValueTask<LongRange> GetSegmentRangeSlowAsync(int index, CancellationToken ct)
    {
        var header = await _client.GetSegmentYencHeaderAsync(_fileSegmentIds[index], ct).ConfigureAwait(false);
        var range = new LongRange(header.PartOffset, header.PartOffset + header.PartSize);

        // Cache locally for this stream instance
        _segmentRangeCache[index] = range;

        return range;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask<InterpolationSearch.Result> SeekSegmentAsync(long byteOffset, CancellationToken ct)
    {
        // Single segment optimization - no search needed, return immediately
        if (_isSingleSegment)
        {
            return new ValueTask<InterpolationSearch.Result>(
                new InterpolationSearch.Result(0, _singleSegmentRange));
        }

        return new ValueTask<InterpolationSearch.Result>(
            InterpolationSearch.Find(
                byteOffset,
                new LongRange(0, _fileSegmentIds.Length),
                new LongRange(0, _fileSize),
                (guess) => GetSegmentRangeAsync(guess, ct),
                ct
            ));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async ValueTask<CombinedStream> GetFileStreamAsync(long rangeStart, CancellationToken ct)
    {
        // Fast path: starting from beginning
        if (rangeStart == 0)
        {
            return CreateCombinedStream(0, ct);
        }

        var foundSegment = await SeekSegmentAsync(rangeStart, ct).ConfigureAwait(false);
        var stream = CreateCombinedStream(foundSegment.FoundIndex, ct);
        var bytesToDiscard = rangeStart - foundSegment.FoundByteRange.StartInclusive;

        if (bytesToDiscard > 0)
        {
            await stream.DiscardBytesAsync(bytesToDiscard).ConfigureAwait(false);
        }

        return stream;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private CombinedStream CreateCombinedStream(int firstSegmentIndex, CancellationToken ct)
    {
        // Use Memory<string> slice for zero-allocation segment access
        var segments = _fileSegmentIds.AsMemory(firstSegmentIndex);
        return new CombinedStream(
            CreateSegmentStreams(segments, ct)
                .WithConcurrency(_concurrentConnections)
        );
    }

    private IEnumerable<Task<Stream>> CreateSegmentStreams(ReadOnlyMemory<string> segments, CancellationToken ct)
    {
        var span = segments.Span;
        for (var i = 0; i < span.Length; i++)
        {
            var segmentId = span[i];
            yield return GetSegmentStreamAsync(segmentId, ct);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async Task<Stream> GetSegmentStreamAsync(string segmentId, CancellationToken ct)
    {
        return await _client.GetSegmentStreamAsync(segmentId, false, ct).ConfigureAwait(false);
    }

    protected override void Dispose(bool disposing)
    {
        if (_disposed) return;
        if (disposing)
        {
            _innerStream?.Dispose();
        }
        _disposed = true;
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        if (_innerStream != null)
        {
            await _innerStream.DisposeAsync().ConfigureAwait(false);
        }
        _disposed = true;
        GC.SuppressFinalize(this);
    }
}