using System.Runtime.CompilerServices;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Models;

namespace NzbWebDAV.Utils;

/// <summary>
/// High-performance interpolation search for finding byte positions in segmented files.
/// Uses interpolation (estimation based on uniform distribution) for O(log log n) average case.
/// </summary>
public static class InterpolationSearch
{
    // Pre-allocated exception message to avoid string allocation on hot path
    private const string CorruptFileMessage = "Corrupt file. Cannot find byte position.";

    /// <summary>
    /// Synchronous version for cases where the range lookup is synchronous.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Result Find(
        long searchByte,
        LongRange indexRangeToSearch,
        LongRange byteRangeToSearch,
        Func<int, LongRange> getByteRangeOfGuessedIndex)
    {
        return Find(
            searchByte,
            indexRangeToSearch,
            byteRangeToSearch,
            guess => new ValueTask<LongRange>(getByteRangeOfGuessedIndex(guess)),
            CancellationToken.None
        ).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Asynchronous interpolation search with ValueTask support for minimal allocations.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<Result> Find(
        long searchByte,
        LongRange indexRangeToSearch,
        LongRange byteRangeToSearch,
        Func<int, ValueTask<LongRange>> getByteRangeOfGuessedIndex,
        CancellationToken cancellationToken)
    {
        // Fast path: single index - no search needed, avoid async overhead
        if (indexRangeToSearch.Count == 1)
        {
            if (!byteRangeToSearch.Contains(searchByte))
                ThrowCorruptFile(searchByte);
            return Task.FromResult(new Result((int)indexRangeToSearch.StartInclusive, byteRangeToSearch));
        }

        // Fast path: two indices - simple binary choice
        if (indexRangeToSearch.Count == 2)
        {
            return FindBinaryAsync(searchByte, indexRangeToSearch, byteRangeToSearch, getByteRangeOfGuessedIndex, cancellationToken);
        }

        return FindCoreAsync(searchByte, indexRangeToSearch, byteRangeToSearch, getByteRangeOfGuessedIndex, cancellationToken);
    }

    /// <summary>
    /// Optimized path for binary search (2 elements).
    /// </summary>
    private static async Task<Result> FindBinaryAsync(
        long searchByte,
        LongRange indexRangeToSearch,
        LongRange byteRangeToSearch,
        Func<int, ValueTask<LongRange>> getByteRangeOfGuessedIndex,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var firstIndex = (int)indexRangeToSearch.StartInclusive;
        var firstRange = await getByteRangeOfGuessedIndex(firstIndex).ConfigureAwait(false);

        if (firstRange.Contains(searchByte))
            return new Result(firstIndex, firstRange);

        var secondIndex = firstIndex + 1;
        var secondRange = await getByteRangeOfGuessedIndex(secondIndex).ConfigureAwait(false);

        if (secondRange.Contains(searchByte))
            return new Result(secondIndex, secondRange);

        ThrowCorruptFile(searchByte);
        return default; // Never reached
    }

    private static async Task<Result> FindCoreAsync(
        long searchByte,
        LongRange indexRangeToSearch,
        LongRange byteRangeToSearch,
        Func<int, ValueTask<LongRange>> getByteRangeOfGuessedIndex,
        CancellationToken cancellationToken)
    {
        // Local copies for mutation
        var indexRange = indexRangeToSearch;
        var byteRange = byteRangeToSearch;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Validate search is possible
            if (!byteRange.Contains(searchByte) || indexRange.Count <= 0)
                ThrowCorruptFile(searchByte);

            // Interpolation guess - estimate position based on uniform distribution
            var searchByteFromStart = searchByte - byteRange.StartInclusive;
            var bytesPerIndex = (double)byteRange.Count / indexRange.Count;
            var guessFromStart = (long)(searchByteFromStart / bytesPerIndex);
            var guessedIndex = (int)(indexRange.StartInclusive + guessFromStart);

            // Clamp guess to valid range (handles edge cases)
            guessedIndex = Math.Clamp(guessedIndex, (int)indexRange.StartInclusive, (int)indexRange.EndExclusive - 1);

            var guessedRange = await getByteRangeOfGuessedIndex(guessedIndex).ConfigureAwait(false);

            // Validate result is within search space
            if (!guessedRange.IsContainedWithin(byteRange))
                ThrowCorruptFile(searchByte);

            // Found it
            if (guessedRange.Contains(searchByte))
                return new Result(guessedIndex, guessedRange);

            // Guessed too low - search higher
            if (guessedRange.EndExclusive <= searchByte)
            {
                indexRange = new LongRange(guessedIndex + 1, indexRange.EndExclusive);
                byteRange = new LongRange(guessedRange.EndExclusive, byteRange.EndExclusive);
            }
            // Guessed too high - search lower
            else
            {
                indexRange = new LongRange(indexRange.StartInclusive, guessedIndex);
                byteRange = new LongRange(byteRange.StartInclusive, guessedRange.StartInclusive);
            }
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowCorruptFile(long searchByte)
    {
        throw new SeekPositionNotFoundException($"{CorruptFileMessage} Position: {searchByte}");
    }

    /// <summary>
    /// Result of an interpolation search. Uses readonly record struct to avoid heap allocations.
    /// </summary>
    public readonly record struct Result(int FoundIndex, LongRange FoundByteRange);
}