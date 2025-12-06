using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace NzbWebDAV.Models;

/// <summary>
/// A high-performance range of long values.
/// Uses readonly record struct for stack allocation instead of heap.
/// Optimized for hot-path operations in stream seeking.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public readonly record struct LongRange(long StartInclusive, long EndExclusive)
{
    /// <summary>
    /// Gets the number of values in the range.
    /// </summary>
    public long Count
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => EndExclusive - StartInclusive;
    }

    /// <summary>
    /// Checks if a value is within the range using unsigned arithmetic for branch-free comparison.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Contains(long value) =>
        (ulong)(value - StartInclusive) < (ulong)(EndExclusive - StartInclusive);

    /// <summary>
    /// Checks if another range is completely contained within this range.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Contains(LongRange range) =>
        range.StartInclusive >= StartInclusive && range.EndExclusive <= EndExclusive;

    /// <summary>
    /// Checks if this range is completely contained within another range.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsContainedWithin(LongRange range) =>
        range.Contains(this);

    /// <summary>
    /// Checks if two ranges overlap.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Overlaps(LongRange other) =>
        StartInclusive < other.EndExclusive && EndExclusive > other.StartInclusive;

    /// <summary>
    /// Creates a range from a start position and size.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static LongRange FromStartAndSize(long startInclusive, long size) =>
        new(startInclusive, startInclusive + size);

    /// <summary>
    /// Checks if the range is empty (end <= start).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsEmpty() => EndExclusive <= StartInclusive;

    /// <summary>
    /// Clamps a value to be within this range.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Clamp(long value)
    {
        if (value < StartInclusive) return StartInclusive;
        if (value >= EndExclusive) return EndExclusive - 1;
        return value;
    }

    /// <summary>
    /// Gets the intersection of two ranges, or an empty range if they don't overlap.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public LongRange Intersect(LongRange other)
    {
        var start = Math.Max(StartInclusive, other.StartInclusive);
        var end = Math.Min(EndExclusive, other.EndExclusive);
        return start < end ? new LongRange(start, end) : default;
    }

    /// <summary>
    /// Static empty range instance.
    /// </summary>
    public static readonly LongRange Empty = default;
}