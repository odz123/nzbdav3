using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using NzbWebDAV.Database;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Utils;

namespace NzbWebDAV.Config;

/// <summary>
/// High-performance configuration manager with lock-free reads.
/// Uses immutable frozen dictionary snapshots for thread-safe access without locking.
/// </summary>
public sealed class ConfigManager
{
    // Immutable snapshot of configuration - replaced atomically on updates
    // FrozenDictionary provides faster lookups than regular Dictionary
    private volatile FrozenDictionary<string, string> _configSnapshot = FrozenDictionary<string, string>.Empty;

    // Lock only used for writes (rare operation)
    private readonly object _writeLock = new();

    public event EventHandler<ConfigEventArgs>? OnConfigChanged;

    public async Task LoadConfig()
    {
        await using var dbContext = new DavDatabaseContext();
        // Use projection to avoid loading full entities
        var configItems = await dbContext.ConfigItems
            .Select(x => new { x.ConfigName, x.ConfigValue })
            .ToListAsync()
            .ConfigureAwait(false);

        var newConfig = configItems.ToDictionary(x => x.ConfigName, x => x.ConfigValue);

        lock (_writeLock)
        {
            _configSnapshot = newConfig.ToFrozenDictionary();
        }
    }

    /// <summary>
    /// Gets a configuration value. Lock-free read using immutable snapshot.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string? GetConfigValue(string configName)
    {
        // Lock-free read - volatile ensures we see the latest snapshot
        return _configSnapshot.TryGetValue(configName, out var value) ? value : null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T? GetConfigValue<T>(string configName)
    {
        var rawValue = StringUtil.EmptyToNull(GetConfigValue(configName));
        return rawValue == null ? default : JsonSerializer.Deserialize<T>(rawValue);
    }

    public void UpdateValues(List<ConfigItem> configItems)
    {
        ConfigEventArgs? eventArgs;

        lock (_writeLock)
        {
            // Create mutable copy, update, then freeze
            var mutableConfig = _configSnapshot.ToDictionary(x => x.Key, x => x.Value);
            foreach (var configItem in configItems)
            {
                mutableConfig[configItem.ConfigName] = configItem.ConfigValue;
            }

            eventArgs = new ConfigEventArgs
            {
                ChangedConfig = configItems.ToDictionary(x => x.ConfigName, x => x.ConfigValue),
                NewConfig = new Dictionary<string, string>(mutableConfig)
            };

            // Atomically replace with new immutable snapshot
            _configSnapshot = mutableConfig.ToFrozenDictionary();
        }

        // Invoke event outside the lock to prevent potential deadlocks
        OnConfigChanged?.Invoke(this, eventArgs);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string GetRcloneMountDir()
    {
        var mountDir = StringUtil.EmptyToNull(GetConfigValue("rclone.mount-dir"))
               ?? StringUtil.EmptyToNull(Environment.GetEnvironmentVariable("MOUNT_DIR"))
               ?? "/mnt/nzbdav";
        if (mountDir.EndsWith('/')) mountDir = mountDir.TrimEnd('/');
        return mountDir;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string GetApiKey()
    {
        return StringUtil.EmptyToNull(GetConfigValue("api.key"))
               ?? EnvironmentUtil.GetVariable("FRONTEND_BACKEND_API_KEY");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string GetStrmKey()
    {
        return GetConfigValue("api.strm-key")
               ?? throw new InvalidOperationException("The `api.strm-key` config does not exist.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string GetApiCategories()
    {
        return StringUtil.EmptyToNull(GetConfigValue("api.categories"))
               ?? StringUtil.EmptyToNull(Environment.GetEnvironmentVariable("CATEGORIES"))
               ?? "audio,software,tv,movies";
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string GetManualUploadCategory()
    {
        return StringUtil.EmptyToNull(GetConfigValue("api.manual-category"))
               ?? "uncategorized";
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetConnectionsPerStream()
    {
        return int.Parse(
            StringUtil.EmptyToNull(GetConfigValue("usenet.connections-per-stream"))
            ?? StringUtil.EmptyToNull(Environment.GetEnvironmentVariable("CONNECTIONS_PER_STREAM"))
            ?? "5"
        );
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string? GetWebdavUser()
    {
        return StringUtil.EmptyToNull(GetConfigValue("webdav.user"))
               ?? StringUtil.EmptyToNull(Environment.GetEnvironmentVariable("WEBDAV_USER"))
               ?? "admin";
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string? GetWebdavPasswordHash()
    {
        var hashedPass = StringUtil.EmptyToNull(GetConfigValue("webdav.pass"));
        if (hashedPass != null) return hashedPass;
        var pass = Environment.GetEnvironmentVariable("WEBDAV_PASSWORD");
        if (pass != null) return PasswordUtil.Hash(pass);
        return null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsEnsureImportableVideoEnabled()
    {
        var configValue = StringUtil.EmptyToNull(GetConfigValue("api.ensure-importable-video"));
        return configValue != null ? bool.Parse(configValue) : true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ShowHiddenWebdavFiles()
    {
        var configValue = StringUtil.EmptyToNull(GetConfigValue("webdav.show-hidden-files"));
        return configValue != null && bool.Parse(configValue);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string? GetLibraryDir()
    {
        return StringUtil.EmptyToNull(GetConfigValue("media.library-dir"));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetMaxQueueConnections()
    {
        return int.Parse(
            StringUtil.EmptyToNull(GetConfigValue("api.max-queue-connections"))
            ?? GetUsenetProviderConfig().TotalPooledConnections.ToString()
        );
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsEnforceReadonlyWebdavEnabled()
    {
        var configValue = StringUtil.EmptyToNull(GetConfigValue("webdav.enforce-readonly"));
        return configValue == null || bool.Parse(configValue);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsEnsureArticleExistenceEnabled()
    {
        var configValue = StringUtil.EmptyToNull(GetConfigValue("api.ensure-article-existence"));
        return configValue != null && bool.Parse(configValue);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsPreviewPar2FilesEnabled()
    {
        var configValue = StringUtil.EmptyToNull(GetConfigValue("webdav.preview-par2-files"));
        return configValue != null && bool.Parse(configValue);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsIgnoreSabHistoryLimitEnabled()
    {
        var configValue = StringUtil.EmptyToNull(GetConfigValue("api.ignore-history-limit"));
        return configValue == null || bool.Parse(configValue);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetMaxRepairConnections()
    {
        return int.Parse(
            StringUtil.EmptyToNull(GetConfigValue("repair.connections"))
            ?? GetUsenetProviderConfig().TotalPooledConnections.ToString()
        );
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRepairJobEnabled()
    {
        var configValue = StringUtil.EmptyToNull(GetConfigValue("repair.enable"));
        var isRepairJobEnabled = configValue != null && bool.Parse(configValue);
        return isRepairJobEnabled
               && GetMaxRepairConnections() > 0
               && GetLibraryDir() != null
               && GetArrConfig().GetInstanceCount() > 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ArrConfig GetArrConfig()
    {
        return GetConfigValue<ArrConfig>("arr.instances") ?? new ArrConfig();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public UsenetProviderConfig GetUsenetProviderConfig()
    {
        return GetConfigValue<UsenetProviderConfig>("usenet.providers") ?? new UsenetProviderConfig();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string GetDuplicateNzbBehavior()
    {
        return GetConfigValue("api.duplicate-nzb-behavior") ?? "increment";
    }

    public HashSet<string> GetBlacklistedExtensions()
    {
        var defaultValue = ".nfo, .par2, .sfv";
        return (GetConfigValue("api.download-extension-blacklist") ?? defaultValue)
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(x => x.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.ToLower())
            .ToHashSet();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string GetImportStrategy()
    {
        return GetConfigValue("api.import-strategy") ?? "symlinks";
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string GetStrmCompletedDownloadDir()
    {
        return GetConfigValue("api.completed-downloads-dir") ?? "/data/completed-downloads";
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string GetBaseUrl()
    {
        return GetConfigValue("general.base-url") ?? "http://localhost:3000";
    }

    public sealed class ConfigEventArgs : EventArgs
    {
        public Dictionary<string, string> ChangedConfig { get; init; } = new();
        public Dictionary<string, string> NewConfig { get; init; } = new();
    }
}
