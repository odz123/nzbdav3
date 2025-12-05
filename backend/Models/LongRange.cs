using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace NzbWebDAV.Models;

/// <summary>
/// A range of long values. Uses readonly record struct for stack allocation instead of heap.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public readonly record struct LongRange(long StartInclusive, long EndExclusive)
{
    public long Count
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => EndExclusive - StartInclusive;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Contains(long value) =>
        (ulong)(value - StartInclusive) < (ulong)(EndExclusive - StartInclusive);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Contains(LongRange range) =>
        range.StartInclusive >= StartInclusive && range.EndExclusive <= EndExclusive;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsContainedWithin(LongRange range) =>
        range.Contains(this);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static LongRange FromStartAndSize(long startInclusive, long size) =>
        new(startInclusive, startInclusive + size);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsEmpty() => EndExclusive <= StartInclusive;
}