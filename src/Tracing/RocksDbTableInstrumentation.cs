using System;
using System.Diagnostics;

namespace RocksDbTable.Tracing;

/// <summary>
/// Provides tracing instrumentation for RocksDbTable.
/// </summary>
public static class RocksDbTableInstrumentation
{
    /// <summary>
    /// The name of the activity source.
    /// </summary>
    public const string ActivitySourceName = "RocksDbTable";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    internal static Activity? SetKeyToActivity(this Activity? activity, ReadOnlySpan<byte> key)
    {
        if (activity?.IsAllDataRequested == true)
        {
            activity.SetTag("key", Convert.ToBase64String(key));
        }

        return activity;
    }

    internal static Activity? SetValueToActivity(this Activity? activity, ReadOnlySpan<byte> value)
    {
        if (activity?.IsAllDataRequested == true)
        {
            activity.SetTag("value", Convert.ToBase64String(value));
        }

        return activity;
    }

    internal static Activity? SetRangeKeysToActivity(this Activity? activity, ReadOnlySpan<byte> startKey, ReadOnlySpan<byte> endKey)
    {
        if (activity?.IsAllDataRequested == true)
        {
            activity.SetTag("startKey", Convert.ToBase64String(startKey));
            activity.SetTag("endKey", Convert.ToBase64String(endKey));
        }

        return activity;
    }
}