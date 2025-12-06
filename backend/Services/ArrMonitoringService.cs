using System.Runtime.CompilerServices;
using NzbWebDAV.Clients.RadarrSonarr;
using NzbWebDAV.Clients.RadarrSonarr.BaseModels;
using NzbWebDAV.Config;
using NzbWebDAV.Utils;
using Serilog;

namespace NzbWebDAV.Services;

/// <summary>
/// High-performance Radarr/Sonarr queue monitoring service.
/// Automatically handles stuck queue items according to configuration rules.
/// Uses efficient polling with reduced allocations.
/// </summary>
public sealed class ArrMonitoringService
{
    private readonly ConfigManager _configManager;
    private readonly CancellationToken _cancellationToken = SigtermUtil.GetCancellationToken();

    // Pre-computed delays
    private static readonly TimeSpan MonitorInterval = TimeSpan.FromSeconds(10);

    public ArrMonitoringService(ConfigManager configManager)
    {
        _configManager = configManager;
        _ = StartMonitoringService();
    }

    private async Task StartMonitoringService()
    {
        while (!_cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Delay at the start to allow system to initialize
                await Task.Delay(MonitorInterval, _cancellationToken).ConfigureAwait(false);

                // If all queue-actions are disabled, skip processing
                var arrConfig = _configManager.GetArrConfig();
                if (!HasActiveRules(arrConfig.QueueRules))
                    continue;

                // Handle stuck queue items for each configured arr client
                var arrClients = arrConfig.GetArrClients();
                foreach (var arrClient in arrClients)
                {
                    await HandleStuckQueueItems(arrConfig, arrClient).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (_cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception e)
            {
                Log.Error(e, "Unexpected error in ArrMonitoringService: {Message}", e.Message);
            }
        }
    }

    /// <summary>
    /// Checks if any queue rules have active actions (not DoNothing).
    /// Avoids LINQ allocation by using a simple loop.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool HasActiveRules(IReadOnlyList<ArrConfig.QueueRule> rules)
    {
        for (var i = 0; i < rules.Count; i++)
        {
            if (rules[i].Action != ArrConfig.QueueAction.DoNothing)
                return true;
        }
        return false;
    }

    private async Task HandleStuckQueueItems(ArrConfig arrConfig, ArrClient client)
    {
        try
        {
            var queueStatus = await client.GetQueueStatusAsync().ConfigureAwait(false);
            if (queueStatus is { Warnings: false, UnknownWarnings: false }) return;

            var queue = await client.GetQueueAsync().ConfigureAwait(false);
            var rules = arrConfig.QueueRules;

            foreach (var record in queue.Records)
            {
                if (HasMatchingRule(record, rules))
                {
                    await HandleStuckQueueItem(record, arrConfig, client).ConfigureAwait(false);
                }
            }
        }
        catch (Exception e)
        {
            Log.Error("Error occurred while monitoring queue for `{Host}`: {Message}", client.Host, e.Message);
        }
    }

    /// <summary>
    /// Checks if a record has any matching rule message.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool HasMatchingRule(ArrQueueRecord record, IReadOnlyList<ArrConfig.QueueRule> rules)
    {
        for (var i = 0; i < rules.Count; i++)
        {
            if (record.HasStatusMessage(rules[i].Message))
                return true;
        }
        return false;
    }

    private async Task HandleStuckQueueItem(ArrQueueRecord item, ArrConfig arrConfig, ArrClient client)
    {
        // Find the strongest applicable action
        var action = GetStrongestAction(item, arrConfig.QueueRules);

        if (action == ArrConfig.QueueAction.DoNothing) return;

        await client.DeleteQueueRecord(item.Id, action).ConfigureAwait(false);
        Log.Warning("Resolved stuck queue item `{Title}` from `{Host}` with action `{Action}`",
            item.Title, client.Host, action);
    }

    /// <summary>
    /// Gets the strongest action from matching rules.
    /// Avoids LINQ allocations by using a simple loop.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ArrConfig.QueueAction GetStrongestAction(ArrQueueRecord item, IReadOnlyList<ArrConfig.QueueRule> rules)
    {
        var maxAction = ArrConfig.QueueAction.DoNothing;
        for (var i = 0; i < rules.Count; i++)
        {
            var rule = rules[i];
            if (item.HasStatusMessage(rule.Message) && rule.Action > maxAction)
            {
                maxAction = rule.Action;
            }
        }
        return maxAction;
    }
}
