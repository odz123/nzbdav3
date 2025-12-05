using System.Runtime.ExceptionServices;
using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Clients.Usenet.Models;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Extensions;
using NzbWebDAV.Models;
using NzbWebDAV.Streams;
using Serilog;
using Usenet.Nntp.Responses;
using Usenet.Nzb;
using Usenet.Yenc;

namespace NzbWebDAV.Clients.Usenet;

public class MultiProviderNntpClient(List<MultiConnectionNntpClient> providers) : INntpClient
{
    // Pre-computed list of enabled providers to avoid filtering on every call
    private List<MultiConnectionNntpClient> _enabledProviders = providers
        .Where(x => x.ProviderType != ProviderType.Disabled)
        .ToList();

    public Task<bool> ConnectAsync(string host, int port, bool useSsl, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Please connect within the connectionFactory");
    }

    public Task<bool> AuthenticateAsync(string user, string pass, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Please authenticate within the connectionFactory");
    }

    public Task<NntpStatResponse> StatAsync(string segmentId, CancellationToken cancellationToken)
    {
        return RunFromPoolWithBackup(connection => connection.StatAsync(segmentId, cancellationToken),
            cancellationToken);
    }

    public Task<NntpDateResponse> DateAsync(CancellationToken cancellationToken)
    {
        return RunFromPoolWithBackup(connection => connection.DateAsync(cancellationToken), cancellationToken);
    }

    public Task<UsenetArticleHeaders> GetArticleHeadersAsync(string segmentId, CancellationToken cancellationToken)
    {
        return RunFromPoolWithBackup(connection => connection.GetArticleHeadersAsync(segmentId, cancellationToken),
            cancellationToken);
    }

    public Task<YencHeaderStream> GetSegmentStreamAsync(string segmentId, bool includeHeaders,
        CancellationToken cancellationToken)
    {
        return RunFromPoolWithBackup(
            connection => connection.GetSegmentStreamAsync(segmentId, includeHeaders, cancellationToken),
            cancellationToken);
    }

    public Task<YencHeader> GetSegmentYencHeaderAsync(string segmentId, CancellationToken cancellationToken)
    {
        return RunFromPoolWithBackup(connection => connection.GetSegmentYencHeaderAsync(segmentId, cancellationToken),
            cancellationToken);
    }

    public Task<long> GetFileSizeAsync(NzbFile file, CancellationToken cancellationToken)
    {
        return RunFromPoolWithBackup(connection => connection.GetFileSizeAsync(file, cancellationToken),
            cancellationToken);
    }

    public Task WaitForReady(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    private async Task<T> RunFromPoolWithBackup<T>
    (
        Func<INntpClient, Task<T>> task,
        CancellationToken cancellationToken
    )
    {
        ExceptionDispatchInfo? lastException = null;
        var lastSuccessfulProviderContext = cancellationToken.GetContext<LastSuccessfulProviderContext>();
        var lastSuccessfulProvider = lastSuccessfulProviderContext?.Provider;
        T? result = default;

        // Fast path: try the preferred provider first without any allocation
        if (lastSuccessfulProvider is not null && lastSuccessfulProvider.ProviderType != ProviderType.Disabled)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                result = await task.Invoke(lastSuccessfulProvider).ConfigureAwait(false);
                if (result is NntpStatResponse r && r.ResponseType != NntpStatResponseType.ArticleExists)
                    throw new UsenetArticleNotFoundException(r.MessageId.Value);
                return result;
            }
            catch (Exception e) when (e is not OperationCanceledException and not TaskCanceledException)
            {
                lastException = ExceptionDispatchInfo.Capture(e);
            }
        }

        // Fallback: try other providers
        foreach (var provider in GetOrderedProviders(lastSuccessfulProvider))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (lastException is not null && lastException.SourceException is not UsenetArticleNotFoundException)
            {
                var msg = lastException.SourceException.Message;
                Log.Debug($"Encountered error during NNTP Operation: `{msg}`. Trying another provider.");
            }

            try
            {
                result = await task.Invoke(provider).ConfigureAwait(false);
                if (result is NntpStatResponse r && r.ResponseType != NntpStatResponseType.ArticleExists)
                    throw new UsenetArticleNotFoundException(r.MessageId.Value);

                if (lastSuccessfulProviderContext is not null && lastSuccessfulProvider != provider)
                    lastSuccessfulProviderContext.Provider = provider;
                return result;
            }
            catch (Exception e) when (e is not OperationCanceledException and not TaskCanceledException)
            {
                lastException = ExceptionDispatchInfo.Capture(e);
            }
        }

        if (result is NntpStatResponse)
            return result;

        lastException?.Throw();
        throw new Exception("There are no usenet providers configured.");
    }

    /// <summary>
    /// Gets providers ordered by availability without LINQ allocations.
    /// </summary>
    private IEnumerable<MultiConnectionNntpClient> GetOrderedProviders(MultiConnectionNntpClient? excludeProvider)
    {
        var enabledProviders = _enabledProviders;
        var count = enabledProviders.Count;

        if (count == 0) yield break;

        // Single provider fast path
        if (count == 1)
        {
            var provider = enabledProviders[0];
            if (provider != excludeProvider)
                yield return provider;
            yield break;
        }

        // For 2-3 providers (common case), use direct comparison to avoid sorting allocations
        if (count <= 3)
        {
            // Find best provider by availability metrics
            MultiConnectionNntpClient? best = null;
            foreach (var provider in enabledProviders)
            {
                if (provider == excludeProvider) continue;
                if (best == null || IsBetterProvider(provider, best))
                    best = provider;
            }

            if (best != null)
            {
                yield return best;
                foreach (var provider in enabledProviders)
                {
                    if (provider != excludeProvider && provider != best)
                        yield return provider;
                }
            }
            yield break;
        }

        // For larger collections, fall back to LINQ (rare case)
        foreach (var provider in enabledProviders
            .Where(x => x != excludeProvider)
            .OrderBy(x => x.ProviderType)
            .ThenByDescending(x => x.IdleConnections)
            .ThenByDescending(x => x.RemainingSemaphoreSlots))
        {
            yield return provider;
        }
    }

    private static bool IsBetterProvider(MultiConnectionNntpClient a, MultiConnectionNntpClient b)
    {
        // Lower ProviderType is better (Primary < Secondary)
        if (a.ProviderType != b.ProviderType)
            return a.ProviderType < b.ProviderType;
        // More idle connections is better
        if (a.IdleConnections != b.IdleConnections)
            return a.IdleConnections > b.IdleConnections;
        // More remaining semaphore slots is better
        return a.RemainingSemaphoreSlots > b.RemainingSemaphoreSlots;
    }

    /// <summary>
    /// Refreshes the enabled providers list when configuration changes.
    /// </summary>
    public void RefreshEnabledProviders()
    {
        _enabledProviders = providers
            .Where(x => x.ProviderType != ProviderType.Disabled)
            .ToList();
    }

    public void Dispose()
    {
        foreach (var provider in providers)
            provider.Dispose();
        GC.SuppressFinalize(this);
    }
}