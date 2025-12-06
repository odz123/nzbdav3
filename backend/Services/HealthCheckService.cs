using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Config;
using NzbWebDAV.Database;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Extensions;
using NzbWebDAV.Utils;
using NzbWebDAV.Websocket;
using Serilog;

namespace NzbWebDAV.Services;

/// <summary>
/// High-performance health check monitoring service with event-driven configuration updates.
/// Uses lock-free patterns and efficient polling with adaptive delays.
/// </summary>
public sealed class HealthCheckService
{
    private readonly ConfigManager _configManager;
    private readonly UsenetStreamingClient _usenetClient;
    private readonly WebsocketManager _websocketManager;
    private readonly CancellationToken _cancellationToken = SigtermUtil.GetCancellationToken();

    // Use ConcurrentDictionary for lock-free reads - much faster than HashSet with lock
    // Pre-allocate with expected capacity to avoid resizing
    private ConcurrentDictionary<string, byte> _missingSegmentIds = new(
        concurrencyLevel: Environment.ProcessorCount,
        capacity: 1024);

    // Pre-computed delays for efficiency
    private static readonly TimeSpan ShortDelay = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan IdleDelay = TimeSpan.FromSeconds(30);

    public HealthCheckService(
        ConfigManager configManager,
        UsenetStreamingClient usenetClient,
        WebsocketManager websocketManager)
    {
        _configManager = configManager;
        _usenetClient = usenetClient;
        _websocketManager = websocketManager;

        _configManager.OnConfigChanged += OnConfigChanged;
        _ = StartMonitoringService();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnConfigChanged(object? sender, ConfigManager.ConfigEventArgs e)
    {
        // When usenet host changes, clear the missing segments cache atomically
        if (!e.ChangedConfig.ContainsKey("usenet.host")) return;
        Interlocked.Exchange(ref _missingSegmentIds, new ConcurrentDictionary<string, byte>(
            concurrencyLevel: Environment.ProcessorCount,
            capacity: 1024));
    }

    private async Task StartMonitoringService()
    {
        while (!_cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Fast check if repair job is disabled
                if (!_configManager.IsRepairJobEnabled())
                {
                    await Task.Delay(IdleDelay, _cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // Get concurrency
                var concurrency = _configManager.GetMaxRepairConnections();

                // Set reserved-connections context
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cancellationToken);
                var providerConfig = _configManager.GetUsenetProviderConfig();
                var reservedConnections = providerConfig.TotalPooledConnections - concurrency;
                using var _ = cts.Token.SetScopedContext(new ReservedPooledConnectionsContext(reservedConnections));

                // Get the davItem to health-check
                await using var dbContext = new DavDatabaseContext();
                var dbClient = new DavDatabaseClient(dbContext);
                var currentDateTime = DateTimeOffset.UtcNow;
                var davItem = await GetHealthCheckQueueItems(dbClient)
                    .Where(x => x.NextHealthCheck == null || x.NextHealthCheck < currentDateTime)
                    .FirstOrDefaultAsync(cts.Token).ConfigureAwait(false);

                // If there is no item to health-check, use longer delay
                if (davItem == null)
                {
                    await Task.Delay(IdleDelay, cts.Token).ConfigureAwait(false);
                    continue;
                }

                // Perform the health check
                await PerformHealthCheck(davItem, dbClient, concurrency, cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception e)
            {
                Log.Error(e, "Unexpected error performing background health checks: {Message}", e.Message);
                await Task.Delay(ShortDelay, _cancellationToken).ConfigureAwait(false);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IOrderedQueryable<DavItem> GetHealthCheckQueueItems(DavDatabaseClient dbClient)
    {
        return GetHealthCheckQueueItemsQuery(dbClient)
            .OrderBy(x => x.NextHealthCheck)
            .ThenByDescending(x => x.ReleaseDate)
            .ThenBy(x => x.Id);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IQueryable<DavItem> GetHealthCheckQueueItemsQuery(DavDatabaseClient dbClient)
    {
        const HealthCheckResult.RepairAction actionNeeded = HealthCheckResult.RepairAction.ActionNeeded;
        var healthCheckResults = dbClient.Ctx.HealthCheckResults;
        return dbClient.Ctx.Items
            .Where(x => x.Type == DavItem.ItemType.NzbFile
                        || x.Type == DavItem.ItemType.RarFile
                        || x.Type == DavItem.ItemType.MultipartFile)
            .Where(x => !healthCheckResults.Any(h => h.DavItemId == x.Id && h.RepairStatus == actionNeeded));
    }

    private async Task PerformHealthCheck(
        DavItem davItem,
        DavDatabaseClient dbClient,
        int concurrency,
        CancellationToken ct)
    {
        try
        {
            // Update the release date, if null
            var segments = await GetAllSegments(davItem, dbClient, ct).ConfigureAwait(false);
            if (davItem.ReleaseDate == null)
                await UpdateReleaseDate(davItem, segments, ct).ConfigureAwait(false);

            // Setup progress tracking
            var progressHook = new Progress<int>();
            var debounce = DebounceUtil.CreateDebounce(TimeSpan.FromMilliseconds(200));
            progressHook.ProgressChanged += (_, progress) =>
            {
                var message = $"{davItem.Id}|{progress}";
                debounce(() => _websocketManager.SendMessage(WebsocketTopic.HealthItemProgress, message));
            };

            // Perform health check
            var progress = progressHook.ToPercentage(segments.Count);
            await _usenetClient.CheckAllSegmentsAsync(segments, concurrency, progress, ct).ConfigureAwait(false);
            _ = _websocketManager.SendMessage(WebsocketTopic.HealthItemProgress, $"{davItem.Id}|100");
            _ = _websocketManager.SendMessage(WebsocketTopic.HealthItemProgress, $"{davItem.Id}|done");

            // Update the database
            var utcNow = DateTimeOffset.UtcNow;
            davItem.LastHealthCheck = utcNow;
            davItem.NextHealthCheck = davItem.ReleaseDate + 2 * (utcNow - davItem.ReleaseDate);
            dbClient.Ctx.HealthCheckResults.Add(SendStatus(new HealthCheckResult
            {
                Id = Guid.NewGuid(),
                DavItemId = davItem.Id,
                Path = davItem.Path,
                CreatedAt = utcNow,
                Result = HealthCheckResult.HealthResult.Healthy,
                RepairStatus = HealthCheckResult.RepairAction.None,
                Message = "File is healthy."
            }));
            await dbClient.Ctx.SaveChangesAsync(ct).ConfigureAwait(false);
        }
        catch (UsenetArticleNotFoundException e)
        {
            _ = _websocketManager.SendMessage(WebsocketTopic.HealthItemProgress, $"{davItem.Id}|100");
            _ = _websocketManager.SendMessage(WebsocketTopic.HealthItemProgress, $"{davItem.Id}|done");
            if (FilenameUtil.IsImportantFileType(davItem.Name))
                _missingSegmentIds.TryAdd(e.SegmentId, 0);

            // When usenet article is missing, perform repairs
            await Repair(davItem, dbClient, ct).ConfigureAwait(false);
        }
    }

    private async Task UpdateReleaseDate(DavItem davItem, List<string> segments, CancellationToken ct)
    {
        var firstSegmentId = StringUtil.EmptyToNull(segments.FirstOrDefault());
        if (firstSegmentId == null) return;
        var articleHeaders = await _usenetClient.GetArticleHeadersAsync(firstSegmentId, ct).ConfigureAwait(false);
        davItem.ReleaseDate = articleHeaders.Date;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static async Task<List<string>> GetAllSegments(DavItem davItem, DavDatabaseClient dbClient, CancellationToken ct)
    {
        return davItem.Type switch
        {
            DavItem.ItemType.NzbFile => (await dbClient.GetNzbFileAsync(davItem.Id, ct).ConfigureAwait(false))
                ?.SegmentIds?.ToList() ?? [],
            DavItem.ItemType.RarFile => (await dbClient.GetRarFileAsync(davItem.Id, ct).ConfigureAwait(false))
                ?.RarParts?.SelectMany(x => x.SegmentIds)?.ToList() ?? [],
            DavItem.ItemType.MultipartFile => (await dbClient.GetMultipartFileAsync(davItem.Id, ct).ConfigureAwait(false))
                ?.Metadata?.FileParts?.SelectMany(x => x.SegmentIds)?.ToList() ?? [],
            _ => []
        };
    }

    private async Task Repair(DavItem davItem, DavDatabaseClient dbClient, CancellationToken ct)
    {
        try
        {
            // If the file extension has been marked as ignored, delete it
            var blacklistedExtensions = _configManager.GetBlacklistedExtensions();
            var extension = Path.GetExtension(davItem.Name).ToLowerInvariant();
            if (blacklistedExtensions.Contains(extension))
            {
                dbClient.Ctx.Items.Remove(davItem);
                dbClient.Ctx.HealthCheckResults.Add(SendStatus(new HealthCheckResult
                {
                    Id = Guid.NewGuid(),
                    DavItemId = davItem.Id,
                    Path = davItem.Path,
                    CreatedAt = DateTimeOffset.UtcNow,
                    Result = HealthCheckResult.HealthResult.Unhealthy,
                    RepairStatus = HealthCheckResult.RepairAction.Deleted,
                    Message = "File had missing articles. File extension is marked in settings as ignored (unwanted) file type. Deleted file."
                }));
                await dbClient.Ctx.SaveChangesAsync(ct).ConfigureAwait(false);
                return;
            }

            // If the unhealthy item is unlinked/orphaned, delete it
            var symlinkOrStrmPath = OrganizedLinksUtil.GetLink(davItem, _configManager);
            if (symlinkOrStrmPath == null)
            {
                dbClient.Ctx.Items.Remove(davItem);
                dbClient.Ctx.HealthCheckResults.Add(SendStatus(new HealthCheckResult
                {
                    Id = Guid.NewGuid(),
                    DavItemId = davItem.Id,
                    Path = davItem.Path,
                    CreatedAt = DateTimeOffset.UtcNow,
                    Result = HealthCheckResult.HealthResult.Unhealthy,
                    RepairStatus = HealthCheckResult.RepairAction.Deleted,
                    Message = "File had missing articles. Could not find corresponding symlink or strm-file within Library Dir. Deleted file."
                }));
                await dbClient.Ctx.SaveChangesAsync(ct).ConfigureAwait(false);
                return;
            }

            // If the unhealthy item is linked, find the corresponding arr instance and trigger a new search
            var linkType = symlinkOrStrmPath.EndsWith(".strm", StringComparison.OrdinalIgnoreCase) ? "strm-file" : "symlink";
            foreach (var arrClient in _configManager.GetArrConfig().GetArrClients())
            {
                var rootFolders = await arrClient.GetRootFolders().ConfigureAwait(false);
                if (!rootFolders.Any(x => symlinkOrStrmPath.StartsWith(x.Path!, StringComparison.Ordinal))) continue;

                // If we found a corresponding arr instance, remove and search
                if (await arrClient.RemoveAndSearch(symlinkOrStrmPath).ConfigureAwait(false))
                {
                    dbClient.Ctx.Items.Remove(davItem);
                    dbClient.Ctx.HealthCheckResults.Add(SendStatus(new HealthCheckResult
                    {
                        Id = Guid.NewGuid(),
                        DavItemId = davItem.Id,
                        Path = davItem.Path,
                        CreatedAt = DateTimeOffset.UtcNow,
                        Result = HealthCheckResult.HealthResult.Unhealthy,
                        RepairStatus = HealthCheckResult.RepairAction.Repaired,
                        Message = $"File had missing articles. Corresponding {linkType} found within Library Dir. Triggered new Arr search."
                    }));
                    await dbClient.Ctx.SaveChangesAsync(ct).ConfigureAwait(false);
                    return;
                }

                break;
            }

            // If we could not find a corresponding arr instance, delete both the item and the link-file
            await Task.Run(() => File.Delete(symlinkOrStrmPath), ct).ConfigureAwait(false);
            dbClient.Ctx.Items.Remove(davItem);
            dbClient.Ctx.HealthCheckResults.Add(SendStatus(new HealthCheckResult
            {
                Id = Guid.NewGuid(),
                DavItemId = davItem.Id,
                Path = davItem.Path,
                CreatedAt = DateTimeOffset.UtcNow,
                Result = HealthCheckResult.HealthResult.Unhealthy,
                RepairStatus = HealthCheckResult.RepairAction.Deleted,
                Message = $"File had missing articles. Corresponding {linkType} found within Library Dir. Could not find corresponding Radarr/Sonarr media-item to trigger a new search. Deleted the webdav-file and {linkType}."
            }));
            await dbClient.Ctx.SaveChangesAsync(ct).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // If an error is encountered during repairs, mark the item as unhealthy
            var utcNow = DateTimeOffset.UtcNow;
            davItem.LastHealthCheck = utcNow;
            davItem.NextHealthCheck = null;
            dbClient.Ctx.HealthCheckResults.Add(SendStatus(new HealthCheckResult
            {
                Id = Guid.NewGuid(),
                DavItemId = davItem.Id,
                Path = davItem.Path,
                CreatedAt = utcNow,
                Result = HealthCheckResult.HealthResult.Unhealthy,
                RepairStatus = HealthCheckResult.RepairAction.ActionNeeded,
                Message = $"Error performing file repair: {e.Message}"
            }));
            await dbClient.Ctx.SaveChangesAsync(ct).ConfigureAwait(false);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private HealthCheckResult SendStatus(HealthCheckResult result)
    {
        _ = _websocketManager.SendMessage(
            WebsocketTopic.HealthItemStatus,
            $"{result.DavItemId}|{(int)result.Result}|{(int)result.RepairStatus}"
        );
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void CheckCachedMissingSegmentIds(IEnumerable<string> segmentIds)
    {
        // Lock-free check using ConcurrentDictionary - much faster for reads
        var cache = _missingSegmentIds; // Local copy for performance
        foreach (var segmentId in segmentIds)
            if (cache.ContainsKey(segmentId))
                throw new UsenetArticleNotFoundException(segmentId);
    }
}
