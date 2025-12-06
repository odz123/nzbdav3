using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace NzbWebDAV.Extensions;

/// <summary>
/// High-performance reflection utilities with compiled expression caching.
/// Property/field access is compiled to IL delegates on first use for near-native performance.
/// </summary>
public static class ObjectExtensions
{
    private static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
    private const BindingFlags BindingAttr = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public;

    // Cache compiled property getters for maximum performance
    // Key: (Type, PropertyName), Value: Compiled delegate that gets the property value
    private static readonly ConcurrentDictionary<(Type, string), Func<object, object?>> PropertyGetterCache = new();

    // Cache compiled field getters
    private static readonly ConcurrentDictionary<(Type, string), Func<object, object?>> FieldGetterCache = new();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? GetReflectionProperty(this object obj, string propertyName)
    {
        var type = obj.GetType();
        var key = (type, propertyName);

        // Fast path: check cache first
        if (PropertyGetterCache.TryGetValue(key, out var cachedGetter))
        {
            return cachedGetter(obj);
        }

        // Slow path: create and cache compiled getter
        return GetOrCreatePropertyGetter(obj, type, propertyName, key);
    }

    private static object? GetOrCreatePropertyGetter(object obj, Type type, string propertyName, (Type, string) key)
    {
        var getter = PropertyGetterCache.GetOrAdd(key, static k =>
        {
            var (targetType, propName) = k;
            var prop = targetType.GetProperty(propName, BindingAttr);
            if (prop == null) return static _ => null;

            // Create compiled expression for fast property access
            var parameter = Expression.Parameter(typeof(object), "obj");
            var castToType = Expression.Convert(parameter, targetType);
            var propertyAccess = Expression.Property(castToType, prop);
            var castToObject = Expression.Convert(propertyAccess, typeof(object));
            var lambda = Expression.Lambda<Func<object, object?>>(castToObject, parameter);
            return lambda.Compile();
        });

        return getter(obj);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? GetReflectionField(this object obj, string fieldName)
    {
        var type = obj.GetType();
        var key = (type, fieldName);

        // Fast path: check cache first
        if (FieldGetterCache.TryGetValue(key, out var cachedGetter))
        {
            return cachedGetter(obj);
        }

        // Slow path: create and cache compiled getter
        return GetOrCreateFieldGetter(obj, type, fieldName, key);
    }

    private static object? GetOrCreateFieldGetter(object obj, Type type, string fieldName, (Type, string) key)
    {
        var getter = FieldGetterCache.GetOrAdd(key, static k =>
        {
            var (targetType, fldName) = k;
            var field = targetType.GetField(fldName, BindingAttr);
            if (field == null) return static _ => null;

            // Create compiled expression for fast field access
            var parameter = Expression.Parameter(typeof(object), "obj");
            var castToType = Expression.Convert(parameter, targetType);
            var fieldAccess = Expression.Field(castToType, field);
            var castToObject = Expression.Convert(fieldAccess, typeof(object));
            var lambda = Expression.Lambda<Func<object, object?>>(castToObject, parameter);
            return lambda.Compile();
        });

        return getter(obj);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string ToJson(this object obj)
    {
        return JsonSerializer.Serialize(obj);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string ToIndentedJson(this object obj)
    {
        return JsonSerializer.Serialize(obj, Indented);
    }
}
