using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Caching.Memory;
using NzbWebDAV.Streams;
using Usenet.Nzb;
using Usenet.Yenc;

namespace NzbWebDAV.Clients.Usenet;

public class CachingNntpClient(INntpClient client, MemoryCache cache) : WrappingNntpClient(client)
{
    private readonly MemoryCacheEntryOptions _cacheOptions = new()
    {
        Size = 1,
        SlidingExpiration = TimeSpan.FromHours(6) // Longer expiration - headers don't change
    };

    // Concurrent dictionary for in-flight requests to prevent duplicate fetches
    private readonly ConcurrentDictionary<string, Task<YencHeader>> _pendingRequests = new();

    // Pre-allocated completed task for zero-segment files
    private static readonly Task<long> ZeroSizeTask = Task.FromResult(0L);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override async Task<YencHeaderStream> GetSegmentStreamAsync
    (
        string segmentId,
        bool includeHeaders,
        CancellationToken ct
    )
    {
        var stream = await Client.GetSegmentStreamAsync(segmentId, includeHeaders, ct).ConfigureAwait(false);
        // Cache the header for future seeking operations - thread-safe set
        cache.Set(segmentId, stream.Header, _cacheOptions);
        return stream;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Task<YencHeader> GetSegmentYencHeaderAsync(string segmentId, CancellationToken ct)
    {
        // Fast path: synchronous cache check - avoids async state machine overhead on cache hit
        if (cache.TryGetValue(segmentId, out YencHeader? cachedHeader) && cachedHeader is not null)
        {
            return Task.FromResult(cachedHeader);
        }

        // Check if there's already a pending request for this segment
        if (_pendingRequests.TryGetValue(segmentId, out var pendingTask))
        {
            return pendingTask;
        }

        // Slow path: fetch from server and cache with deduplication
        return GetSegmentYencHeaderWithDeduplicationAsync(segmentId, ct);
    }

    private Task<YencHeader> GetSegmentYencHeaderWithDeduplicationAsync(string segmentId, CancellationToken ct)
    {
        // Use GetOrAdd to prevent duplicate requests atomically
        var task = _pendingRequests.GetOrAdd(segmentId, _ => FetchAndCacheHeaderAsync(segmentId, ct));
        return task;
    }

    private async Task<YencHeader> FetchAndCacheHeaderAsync(string segmentId, CancellationToken ct)
    {
        try
        {
            var header = await Client.GetSegmentYencHeaderAsync(segmentId, ct).ConfigureAwait(false);
            cache.Set(segmentId, header, _cacheOptions);
            return header;
        }
        finally
        {
            // Remove from pending requests once complete
            _pendingRequests.TryRemove(segmentId, out _);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Task<long> GetFileSizeAsync(NzbFile file, CancellationToken ct)
    {
        // Fast path for empty files
        if (file.Segments.Count == 0) return ZeroSizeTask;
        return GetFileSizeCoreAsync(file, ct);
    }

    private async Task<long> GetFileSizeCoreAsync(NzbFile file, CancellationToken ct)
    {
        var header = await GetSegmentYencHeaderAsync(file.Segments[^1].MessageId.Value, ct).ConfigureAwait(false);
        return header.PartOffset + header.PartSize;
    }
}