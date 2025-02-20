using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using RocksDbSharp;
using RocksDbTable.Options;
using RocksDbTable.Serialization;
using RocksDbTable.Tables;
using RocksDbTable.Tracing;
using RocksDbTable.Utils;

namespace RocksDbTable.Core;

internal abstract class KeyValueStoreBase<TKey, TValue> : IKeyValueStoreBase<TKey, TValue>
{
    // ReSharper disable once NotAccessedField.Local Store a reference to options to prevent the GC from collecting ColumnFamilyOptions, since ColumnFamilyOptions can contain unmanaged pointers to delegates.
    private readonly IStoreOptions _options;
    private readonly ThreadLocal<ArrayPoolBufferWriter> _localBufferWriter;
    protected readonly RocksDb RocksDb;
    protected readonly IRockSerializer<TKey> KeySerializer;
    protected readonly ISpanDeserializer<TValue> ValueDeserializer;
    public readonly ColumnFamilyHandle ColumnFamilyHandle;

    protected KeyValueStoreBase(RocksDb rocksDb, IRockSerializer<TKey> keySerializer, IStoreOptions storeOptions, ISpanDeserializer<TValue> valueDeserializer)
    {
        RocksDb = rocksDb;
        KeySerializer = keySerializer;
        _options = storeOptions;
        ColumnFamilyHandle = rocksDb.CreateColumnFamily(storeOptions.ColumnFamilyOptions, storeOptions.ColumnFamilyName);
        ValueDeserializer = valueDeserializer;
        _localBufferWriter = new ThreadLocal<ArrayPoolBufferWriter>(static () => new ArrayPoolBufferWriter(fromPool: false));
    }

    public TValue? GetByKey(ReadOnlySpan<byte> key)
    {
        using var activity = StartActivity(ActivityNames.GetByKeySpan).SetKeyToActivity(key);
        return RocksDb.Get(key, ValueDeserializer, ColumnFamilyHandle);
    }

    public TValue? GetByKey(TKey key)
    {
        using var activity = StartActivity(ActivityNames.GetByKey);
        var keyBuffer = RentBufferWriter();
        try
        {
            KeySerializer.Serialize(keyBuffer, key);
            activity.SetKeyToActivity(keyBuffer.WrittenSpan);
            return RocksDb.Get(keyBuffer.WrittenSpan, ValueDeserializer, ColumnFamilyHandle);
        }
        finally
        {
            ReturnBufferWriter(keyBuffer);
        }
    }

    public bool HasExactKey(TKey uniqueKey)
    {
        using var activity = StartActivity(ActivityNames.HasExactKey);
        var keyBuffer = RentBufferWriter();
        try
        {
            KeySerializer.Serialize(keyBuffer, uniqueKey);
            activity.SetKeyToActivity(keyBuffer.WrittenSpan);
            return RocksDb.HasKey(keyBuffer.WrittenSpan, ColumnFamilyHandle);
        }
        finally
        {
            ReturnBufferWriter(keyBuffer);
        }
    }

    public bool HasAnyKeyByPrefix<TPrefixKey>(TPrefixKey key, IRockSerializer<TPrefixKey> serializer)
    {
        using var activity = StartActivity(ActivityNames.HasAnyKeyByPrefix);
        var prefixBuffer = RentBufferWriter();
        try
        {
            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle);
            serializer.Serialize(prefixBuffer, key);
            activity.SetKeyToActivity(prefixBuffer.WrittenSpan);
            iterator.Seek(prefixBuffer.WrittenSpan);
            if (!iterator.Valid())
            {
                return false;
            }

            return iterator.GetKeySpan().StartsWith(prefixBuffer.WrittenSpan);
        }
        finally
        {
            ReturnBufferWriter(prefixBuffer);
        }
    }

    public IEnumerable<TKey> GetAllKeys()
    {
        using var activity = StartActivity(ActivityNames.GetAllKeys);
        using var iterator = RocksDb.NewIterator(ColumnFamilyHandle);
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
        {
            var fullKey = KeySerializer.Deserialize(iterator.GetKeySpan())!;
            yield return fullKey;
        }
    }

    public IEnumerable<TValue> GetAllValues()
    {
        using var activity = StartActivity(ActivityNames.GetAllValues);
        using var iterator = RocksDb.NewIterator(ColumnFamilyHandle);
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
        {
            var value = ValueDeserializer.Deserialize(iterator.GetValueSpan())!;
            yield return value;
        }
    }

    public IEnumerable<TKey> GetAllKeysByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer)
    {
        using var activity = StartActivity(ActivityNames.GetAllKeysByPrefix);
        var prefixBuffer = RentBufferWriter();
        try
        {
            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle);
            prefixSerializer.Serialize(prefixBuffer, prefixKey);
            activity.SetKeyToActivity(prefixBuffer.WrittenSpan);
            for (iterator.Seek(prefixBuffer.WrittenSpan); iterator.Valid(); iterator.Next())
            {
                var keySpan = iterator.GetKeySpan();
                if (!keySpan.StartsWith(prefixBuffer.WrittenSpan))
                {
                    break;
                }

                var fullKey = KeySerializer.Deserialize(keySpan)!;
                yield return fullKey;
            }
        }
        finally
        {
            ReturnBufferWriter(prefixBuffer);
        }
    }

    public IEnumerable<TValue> GetAllValuesByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer)
    {
        using var activity = StartActivity(ActivityNames.GetAllValuesByPrefix);
        var prefixBuffer = RentBufferWriter();
        try
        {
            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle);
            prefixSerializer.Serialize(prefixBuffer, prefixKey);
            activity.SetKeyToActivity(prefixBuffer.WrittenSpan);
            for (iterator.Seek(prefixBuffer.WrittenSpan); iterator.Valid(); iterator.Next())
            {
                if (!iterator.GetKeySpan().StartsWith(prefixBuffer.WrittenSpan))
                {
                    break;
                }

                yield return ValueDeserializer.Deserialize(iterator.GetValueSpan())!;
            }
        }
        finally
        {
            ReturnBufferWriter(prefixBuffer);
        }
    }

    public TValue? GetFirstValueByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> serializer, SeekMode mode = SeekMode.SeekToFirst)
    {
        using var activity = StartActivity(ActivityNames.GetFirstValueByPrefix);
        var prefixBuffer = RentBufferWriter();
        try
        {
            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle);
            serializer.Serialize(prefixBuffer, prefixKey);
            activity.SetKeyToActivity(prefixBuffer.WrittenSpan);
            if (mode == SeekMode.SeekToPrev)
            {
                iterator.SeekForPrev(prefixBuffer.WrittenSpan);
            }
            else
            {
                iterator.Seek(prefixBuffer.WrittenSpan);
            }

            if (!iterator.Valid())
            {
                return default;
            }

            if (mode == SeekMode.SeekToFirst && !iterator.GetKeySpan().StartsWith(prefixBuffer.WrittenSpan))
            {
                return default;
            }

            var value = ValueDeserializer.Deserialize(iterator.GetValueSpan());
            return value;
        }
        finally
        {
            ReturnBufferWriter(prefixBuffer);
        }
    }

    public IEnumerable<TValue> GetAllValuesByBounds(TKey startInclusive, TKey endExclusive)
    {
        using var activity = StartActivity(ActivityNames.GetAllValuesByBounds);
        var buffer = RentBufferWriter();
        nint startKeyUnmanaged = IntPtr.Zero;
        nint endKeyUnmanaged = IntPtr.Zero;

        try
        {
            KeySerializer.Serialize(buffer, startInclusive);
            var startKeyPoint = new SpanPoint(buffer);

            KeySerializer.Serialize(buffer, endExclusive);
            var endKeyPoint = new SpanPoint(buffer, startKeyPoint);

            var startKeySpan = startKeyPoint.GetWrittenSpan(buffer);
            var endKeySpan = endKeyPoint.GetWrittenSpan(buffer);
            activity.SetRangeKeysToActivity(startKeySpan, endKeySpan);

            var options = new ReadOptions();

            startKeyUnmanaged = CopyToUnmanagedMemory(startKeySpan);
            endKeyUnmanaged = CopyToUnmanagedMemory(endKeySpan);
            unsafe
            {
                options.SetIterateLowerBound((byte*)startKeyUnmanaged, (ulong)startKeySpan.Length);
                options.SetIterateUpperBound((byte*)endKeyUnmanaged, (ulong)endKeySpan.Length);
            }

            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle, options);
            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            {
                yield return ValueDeserializer.Deserialize(iterator.GetValueSpan())!;
            }
        }
        finally
        {
            ReturnBufferWriter(buffer);
            if (startKeyUnmanaged != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(startKeyUnmanaged);
            }

            if (endKeyUnmanaged != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(endKeyUnmanaged);
            }
        }
    }

    protected ArrayPoolBufferWriter RentBufferWriter()
    {
        var localThreadBufferWriter = _localBufferWriter.Value!;
        return localThreadBufferWriter.TryAcquireBuffer()
            ? localThreadBufferWriter
            : ArrayPoolBufferWriter.Pool.Get();
    }

    protected void ReturnBufferWriter(ArrayPoolBufferWriter writer)
    {
        if (writer.FromPool)
        {
            ArrayPoolBufferWriter.Pool.Return(writer);
        }
        else
        {
            writer.Dispose();
        }
    }

    protected Activity? StartActivity(string name)
    {
        var activity = RocksDbTableInstrumentation.ActivitySource.StartActivity(name, ActivityKind.Client);
        activity?.SetTag("columnFamily", _options.ColumnFamilyName);
        return activity;
    }

    /// <summary>
    /// For testing purposes only.
    /// </summary>
    internal byte[]? GetRaw(byte[] key)
    {
        return RocksDb.Get(key, ColumnFamilyHandle);
    }

    internal static ISpanDeserializer<TValue> CreateSpanDeserializerForIndex(IRockSerializer<TValue> valueSerializer, ITableValueProvider<TValue> table, IndexOptions indexOptions)
    {
        return indexOptions.StoreMode == ValueStoreMode.FullValue
            ? new RocksDbSpanDeserializerAdapter<TValue>(valueSerializer)
            : new ByReferenceToTableValueDeserializer<TValue>(table);
    }

    private static nint CopyToUnmanagedMemory(ReadOnlySpan<byte> span)
    {
        var unmanagedPtr = Marshal.AllocHGlobal(span.Length);

        unsafe
        {
            fixed (byte* sourcePtr = span)
            {
                Buffer.MemoryCopy(sourcePtr, (void*)unmanagedPtr, span.Length, span.Length);
            }
        }

        return unmanagedPtr;
    }
}