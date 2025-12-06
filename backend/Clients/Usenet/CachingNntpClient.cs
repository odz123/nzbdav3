using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Caching.Memory;
using NzbWebDAV.Streams;
using Usenet.Nzb;
using Usenet.Yenc;

namespace NzbWebDAV.Clients.Usenet;

/// <summary>
/// High-performance caching wrapper for NNTP client operations.
/// Features:
/// - In-memory caching of YENC headers with 6-hour sliding expiration
/// - Request deduplication to prevent concurrent duplicate fetches
/// - Lock-free concurrent access patterns
/// </summary>
public sealed class CachingNntpClient : WrappingNntpClient
{
    private readonly MemoryCache _cache;

    private readonly MemoryCacheEntryOptions _cacheOptions = new()
    {
        Size = 1,
        SlidingExpiration = TimeSpan.FromHours(6) // Longer expiration - headers don't change
    };

    // Concurrent dictionary for in-flight requests to prevent duplicate fetches
    // Pre-sized for expected concurrency level
    private readonly ConcurrentDictionary<string, Task<YencHeader>> _pendingRequests =
        new(Environment.ProcessorCount * 2, 64);

    // Pre-allocated completed tasks for common cases
    private static readonly Task<long> ZeroSizeTask = Task.FromResult(0L);

    public CachingNntpClient(INntpClient client, MemoryCache cache) : base(client)
    {
        _cache = cache;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override async Task<YencHeaderStream> GetSegmentStreamAsync(
        string segmentId,
        bool includeHeaders,
        CancellationToken ct)
    {
        var stream = await Client.GetSegmentStreamAsync(segmentId, includeHeaders, ct).ConfigureAwait(false);
        // Cache the header for future seeking operations - thread-safe set
        _cache.Set(segmentId, stream.Header, _cacheOptions);
        return stream;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Task<YencHeader> GetSegmentYencHeaderAsync(string segmentId, CancellationToken ct)
    {
        // Fast path 1: synchronous cache check - avoids async state machine overhead on cache hit
        if (_cache.TryGetValue(segmentId, out YencHeader? cachedHeader) && cachedHeader is not null)
        {
            return Task.FromResult(cachedHeader);
        }

        // Fast path 2: check if there's already a pending request for this segment
        if (_pendingRequests.TryGetValue(segmentId, out var pendingTask))
        {
            return pendingTask;
        }

        // Slow path: fetch from server and cache with deduplication
        return GetSegmentYencHeaderWithDeduplicationAsync(segmentId, ct);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Task<YencHeader> GetSegmentYencHeaderWithDeduplicationAsync(string segmentId, CancellationToken ct)
    {
        // Use GetOrAdd to prevent duplicate requests atomically
        // The lambda captures 'ct' but this is acceptable since it's a struct
        return _pendingRequests.GetOrAdd(segmentId, static (id, state) =>
            state.self.FetchAndCacheHeaderAsync(id, state.ct), (self: this, ct));
    }

    private async Task<YencHeader> FetchAndCacheHeaderAsync(string segmentId, CancellationToken ct)
    {
        try
        {
            var header = await Client.GetSegmentYencHeaderAsync(segmentId, ct).ConfigureAwait(false);
            _cache.Set(segmentId, header, _cacheOptions);
            return header;
        }
        finally
        {
            // Remove from pending requests once complete - allows future requests to go through
            _pendingRequests.TryRemove(segmentId, out _);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Task<long> GetFileSizeAsync(NzbFile file, CancellationToken ct)
    {
        // Fast path for empty files - no async overhead
        var segmentCount = file.Segments.Count;
        if (segmentCount == 0) return ZeroSizeTask;

        // Single segment optimization - get last segment's header
        return GetFileSizeCoreAsync(file.Segments[segmentCount - 1].MessageId.Value, ct);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async Task<long> GetFileSizeCoreAsync(string lastSegmentId, CancellationToken ct)
    {
        var header = await GetSegmentYencHeaderAsync(lastSegmentId, ct).ConfigureAwait(false);
        return header.PartOffset + header.PartSize;
    }
}