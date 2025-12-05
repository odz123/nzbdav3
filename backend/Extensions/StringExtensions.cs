using System.Runtime.CompilerServices;

namespace NzbWebDAV.Extensions;

public static class StringExtensions
{
    /// <summary>
    /// Checks if the value matches any of the accepted values using ordinal comparison.
    /// Uses ReadOnlySpan to avoid params array allocation when called with constants.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsAny(this string value, params string[] acceptedValues)
    {
        // Use span-based iteration to avoid enumerator allocation
        var span = acceptedValues.AsSpan();
        for (var i = 0; i < span.Length; i++)
        {
            if (string.Equals(value, span[i], StringComparison.Ordinal))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Removes the prefix from the string if present, using ordinal comparison.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string RemovePrefix(this string value, string prefix)
    {
        return value.StartsWith(prefix, StringComparison.Ordinal) ? value[prefix.Length..] : value;
    }

    /// <summary>
    /// Removes the suffix from the string if present, using ordinal comparison.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string RemoveSuffix(this string value, string suffix)
    {
        return value.EndsWith(suffix, StringComparison.Ordinal) ? value[..^suffix.Length] : value;
    }

    /// <summary>
    /// Fast path check for null or empty strings.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsNullOrEmpty(this string? value)
    {
        return string.IsNullOrEmpty(value);
    }
}