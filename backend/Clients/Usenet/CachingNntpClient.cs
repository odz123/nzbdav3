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

    public override async Task<YencHeaderStream> GetSegmentStreamAsync
    (
        string segmentId,
        bool includeHeaders,
        CancellationToken ct
    )
    {
        var stream = await Client.GetSegmentStreamAsync(segmentId, includeHeaders, ct).ConfigureAwait(false);
        // Cache the header for future seeking operations
        cache.Set(segmentId, stream.Header, _cacheOptions);
        return stream;
    }

    public override Task<YencHeader> GetSegmentYencHeaderAsync(string segmentId, CancellationToken ct)
    {
        // Fast path: synchronous cache check - avoids async state machine overhead on cache hit
        if (cache.TryGetValue(segmentId, out YencHeader? cachedHeader) && cachedHeader is not null)
        {
            return Task.FromResult(cachedHeader);
        }

        // Slow path: fetch from server and cache
        return GetSegmentYencHeaderSlowAsync(segmentId, ct);
    }

    private async Task<YencHeader> GetSegmentYencHeaderSlowAsync(string segmentId, CancellationToken ct)
    {
        var header = await Client.GetSegmentYencHeaderAsync(segmentId, ct).ConfigureAwait(false);
        cache.Set(segmentId, header, _cacheOptions);
        return header;
    }

    public override async Task<long> GetFileSizeAsync(NzbFile file, CancellationToken ct)
    {
        if (file.Segments.Count == 0) return 0;
        var header = await GetSegmentYencHeaderAsync(file.Segments[^1].MessageId.Value, ct).ConfigureAwait(false);
        return header.PartOffset + header.PartSize;
    }
}