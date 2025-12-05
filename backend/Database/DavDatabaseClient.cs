using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using NzbWebDAV.Database.Models;

namespace NzbWebDAV.Database;

public sealed class DavDatabaseClient(DavDatabaseContext ctx)
{
    public DavDatabaseContext Ctx => ctx;

    #region Compiled Queries - Pre-compiled for maximum performance

    private static readonly Func<DavDatabaseContext, Guid, Task<DavItem?>> GetItemByIdQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, Guid id) =>
            ctx.Items.FirstOrDefault(i => i.Id == id));

    private static readonly Func<DavDatabaseContext, Guid, Task<DavNzbFile?>> GetNzbFileByIdQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, Guid id) =>
            ctx.NzbFiles.FirstOrDefault(x => x.Id == id));

    private static readonly Func<DavDatabaseContext, Guid, Task<DavRarFile?>> GetRarFileByIdQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, Guid id) =>
            ctx.RarFiles.FirstOrDefault(x => x.Id == id));

    private static readonly Func<DavDatabaseContext, Guid, Task<DavMultipartFile?>> GetMultipartFileByIdQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, Guid id) =>
            ctx.MultipartFiles.FirstOrDefault(x => x.Id == id));

    private static readonly Func<DavDatabaseContext, Guid, string, Task<DavItem?>> GetDirectoryChildQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, Guid dirId, string childName) =>
            ctx.Items.FirstOrDefault(x => x.ParentId == dirId && x.Name == childName));

    private static readonly Func<DavDatabaseContext, Guid, Task<QueueNzbContents?>> GetQueueNzbContentsByIdQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, Guid id) =>
            ctx.QueueNzbContents.FirstOrDefault(q => q.Id == id));

    private static readonly Func<DavDatabaseContext, Guid, Task<HistoryItem?>> GetHistoryItemByIdQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, Guid id) =>
            ctx.HistoryItems.FirstOrDefault(x => x.Id == id));

    // Additional compiled queries for frequently used operations
    private static readonly Func<DavDatabaseContext, Guid, IAsyncEnumerable<DavItem>> GetDirectoryChildrenStreamQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, Guid dirId) =>
            ctx.Items.Where(x => x.ParentId == dirId));

    private static readonly Func<DavDatabaseContext, string, IAsyncEnumerable<DavItem>> GetFilesByIdPrefixStreamQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, string prefix) =>
            ctx.Items.Where(i => i.IdPrefix == prefix &&
                (i.Type == DavItem.ItemType.NzbFile ||
                 i.Type == DavItem.ItemType.RarFile ||
                 i.Type == DavItem.ItemType.MultipartFile)));

    private static readonly Func<DavDatabaseContext, Task<int>> GetQueueItemsCountAllQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx) => ctx.QueueItems.Count());

    private static readonly Func<DavDatabaseContext, string, Task<int>> GetQueueItemsCountByCategoryQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, string category) =>
            ctx.QueueItems.Count(q => q.Category == category));

    private static readonly Func<DavDatabaseContext, Task<long?>> GetTotalFileSizeQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx) => ctx.Items.Sum(x => x.FileSize));

    // Queue top item compiled query - optimized for hot path
    private static readonly Func<DavDatabaseContext, DateTime, Task<QueueItem?>> GetTopQueueItemQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, DateTime nowTime) =>
            ctx.QueueItems
                .Where(q => q.PauseUntil == null || nowTime >= q.PauseUntil)
                .OrderByDescending(q => q.Priority)
                .ThenBy(q => q.CreatedAt)
                .FirstOrDefault());

    // History compiled queries
    private static readonly Func<DavDatabaseContext, Task<int>> GetHistoryItemsCountQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx) => ctx.HistoryItems.Count());

    private static readonly Func<DavDatabaseContext, string, Task<int>> GetHistoryItemsCountByCategoryQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, string category) =>
            ctx.HistoryItems.Count(h => h.Category == category));

    // Directory children count query
    private static readonly Func<DavDatabaseContext, Guid, Task<int>> GetDirectoryChildrenCountQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, Guid dirId) =>
            ctx.Items.Count(x => x.ParentId == dirId));

    // File existence check query
    private static readonly Func<DavDatabaseContext, Guid, Task<bool>> FileExistsQuery =
        EF.CompileAsyncQuery((DavDatabaseContext ctx, Guid id) =>
            ctx.Items.Any(i => i.Id == id));

    #endregion

    // file
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DavItem?> GetFileById(string id)
    {
        var guid = Guid.Parse(id);
        return GetItemByIdQuery(ctx, guid);
    }

    public async Task<List<DavItem>> GetFilesByIdPrefix(string prefix)
    {
        var results = new List<DavItem>();
        await foreach (var item in GetFilesByIdPrefixStreamQuery(ctx, prefix).ConfigureAwait(false))
        {
            results.Add(item);
        }
        return results;
    }

    // directory
    public async Task<List<DavItem>> GetDirectoryChildrenAsync(Guid dirId, CancellationToken ct = default)
    {
        var results = new List<DavItem>();
        await foreach (var item in GetDirectoryChildrenStreamQuery(ctx, dirId).WithCancellation(ct).ConfigureAwait(false))
        {
            results.Add(item);
        }
        return results;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DavItem?> GetDirectoryChildAsync(Guid dirId, string childName, CancellationToken ct = default)
    {
        return GetDirectoryChildQuery(ctx, dirId, childName);
    }

    public async Task<long> GetRecursiveSize(Guid dirId, CancellationToken ct = default)
    {
        if (dirId == DavItem.Root.Id)
        {
            return await GetTotalFileSizeQuery(ctx).ConfigureAwait(false) ?? 0;
        }

        const string sql = @"
            WITH RECURSIVE RecursiveChildren AS (
                SELECT Id, FileSize
                FROM DavItems
                WHERE ParentId = @parentId

                UNION ALL

                SELECT d.Id, d.FileSize
                FROM DavItems d
                INNER JOIN RecursiveChildren rc ON d.ParentId = rc.Id
            )
            SELECT IFNULL(SUM(FileSize), 0)
            FROM RecursiveChildren;
        ";
        var connection = Ctx.Database.GetDbConnection();
        if (connection.State != System.Data.ConnectionState.Open) await connection.OpenAsync(ct).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var parameter = command.CreateParameter();
        parameter.ParameterName = "@parentId";
        parameter.Value = dirId;
        command.Parameters.Add(parameter);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return Convert.ToInt64(result);
    }

    // nzbfile
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DavNzbFile?> GetNzbFileAsync(Guid id, CancellationToken ct = default)
    {
        return GetNzbFileByIdQuery(ctx, id);
    }

    // rarfile
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DavRarFile?> GetRarFileAsync(Guid id, CancellationToken ct = default)
    {
        return GetRarFileByIdQuery(ctx, id);
    }

    // multipartfile
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DavMultipartFile?> GetMultipartFileAsync(Guid id, CancellationToken ct = default)
    {
        return GetMultipartFileByIdQuery(ctx, id);
    }

    // queue
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async Task<(QueueItem? queueItem, QueueNzbContents? queueNzbContents)> GetTopQueueItem
    (
        CancellationToken ct = default
    )
    {
        var nowTime = DateTime.Now;
        var queueItem = await GetTopQueueItemQuery(ctx, nowTime).ConfigureAwait(false);
        if (queueItem == null) return (null, null);
        var queueNzbContents = await GetQueueNzbContentsByIdQuery(ctx, queueItem.Id).ConfigureAwait(false);
        return (queueItem, queueNzbContents);
    }

    /// <summary>
    /// Gets the count of children in a directory using compiled query.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> GetDirectoryChildrenCountAsync(Guid dirId, CancellationToken ct = default)
    {
        return GetDirectoryChildrenCountQuery(ctx, dirId);
    }

    /// <summary>
    /// Checks if a file exists using compiled query.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<bool> FileExistsAsync(Guid id, CancellationToken ct = default)
    {
        return FileExistsQuery(ctx, id);
    }

    /// <summary>
    /// Gets history items count, optionally by category.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> GetHistoryItemsCountAsync(string? category, CancellationToken ct = default)
    {
        return category != null
            ? GetHistoryItemsCountByCategoryQuery(ctx, category)
            : GetHistoryItemsCountQuery(ctx);
    }

    public Task<QueueItem[]> GetQueueItems
    (
        string? category,
        int start = 0,
        int limit = int.MaxValue,
        CancellationToken ct = default
    )
    {
        var query = category != null
            ? Ctx.QueueItems.Where(q => q.Category == category)
            : Ctx.QueueItems.AsQueryable();
        return query
            .OrderByDescending(q => q.Priority)
            .ThenBy(q => q.CreatedAt)
            .Skip(start)
            .Take(limit)
            .AsNoTracking()
            .ToArrayAsync(cancellationToken: ct);
    }

    public Task<int> GetQueueItemsCount(string? category, CancellationToken ct = default)
    {
        return category != null
            ? GetQueueItemsCountByCategoryQuery(ctx, category)
            : GetQueueItemsCountAllQuery(ctx);
    }

    public Task RemoveQueueItemsAsync(List<Guid> ids, CancellationToken ct = default)
    {
        return Ctx.QueueItems
            .Where(x => ids.Contains(x.Id))
            .ExecuteDeleteAsync(ct);
    }

    // history
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<HistoryItem?> GetHistoryItemAsync(string id)
    {
        return GetHistoryItemByIdQuery(ctx, Guid.Parse(id));
    }

    public async Task RemoveHistoryItemsAsync(List<Guid> ids, bool deleteFiles, CancellationToken ct = default)
    {
        if (deleteFiles)
        {
            await Ctx.Items
                .Where(d => Ctx.HistoryItems
                    .Where(h => ids.Contains(h.Id) && h.DownloadDirId != null)
                    .Select(h => h.DownloadDirId!)
                    .Contains(d.Id))
                .ExecuteDeleteAsync(ct).ConfigureAwait(false);
        }

        await Ctx.HistoryItems
            .Where(x => ids.Contains(x.Id))
            .ExecuteDeleteAsync(ct).ConfigureAwait(false);
    }

    // health check
    public Task<List<HealthCheckStat>> GetHealthCheckStatsAsync
    (
        DateTimeOffset from,
        DateTimeOffset to,
        CancellationToken ct = default
    )
    {
        return Ctx.HealthCheckStats
            .AsNoTracking()
            .Where(h => h.DateStartInclusive >= from && h.DateStartInclusive <= to)
            .GroupBy(h => new { h.Result, h.RepairStatus })
            .Select(g => new HealthCheckStat
            {
                Result = g.Key.Result,
                RepairStatus = g.Key.RepairStatus,
                Count = g.Select(r => r.Count).Sum(),
            })
            .ToListAsync(ct);
    }

    // completed-symlinks
    public Task<List<DavItem>> GetCompletedSymlinkCategoryChildren(string category,
        CancellationToken ct = default)
    {
        return (from historyItem in Ctx.HistoryItems.AsNoTracking()
            where historyItem.Category == category
                  && historyItem.DownloadStatus == HistoryItem.DownloadStatusOption.Completed
                  && historyItem.DownloadDirId != null
            join davItem in Ctx.Items.AsNoTracking() on historyItem.DownloadDirId equals davItem.Id
            where davItem.Type == DavItem.ItemType.Directory
            select davItem).Distinct().ToListAsync(ct);
    }
}