using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using Haqon.RocksDb.Options;
using Haqon.RocksDb.Serialization;
using Haqon.RocksDb.Tables;
using Haqon.RocksDb.Utils;
using RocksDbSharp;

namespace Haqon.RocksDb.Core;

internal abstract class KeyValueStoreBase<TKey, TValue> : IKeyValueStoreBase<TKey, TValue>
{
    // ReSharper disable once NotAccessedField.Local Store a reference to options to prevent the GC from collecting ColumnFamilyOptions, since ColumnFamilyOptions can contain unmanaged pointers to delegates.
    private readonly IStoreOptions _options;
    internal readonly ColumnFamilyHandle ColumnFamilyHandle;
    protected readonly RocksDbSharp.RocksDb RocksDb;
    protected readonly IRockSerializer<TKey> KeySerializer;
    protected readonly ISpanDeserializer<TValue> ValueDeserializer;
    protected readonly ThreadLocal<ArrayPoolBufferWriter> LocalBufferWriter;

    protected KeyValueStoreBase(RocksDbSharp.RocksDb rocksDb, IRockSerializer<TKey> keySerializer, IStoreOptions storeOptions, ISpanDeserializer<TValue> valueDeserializer)
    {
        RocksDb = rocksDb;
        KeySerializer = keySerializer;
        _options = storeOptions;
        ColumnFamilyHandle = rocksDb.CreateColumnFamily(storeOptions.ColumnFamilyOptions, storeOptions.ColumnFamilyName);
        ValueDeserializer = valueDeserializer;
        LocalBufferWriter = new ThreadLocal<ArrayPoolBufferWriter>(static () => new ArrayPoolBufferWriter());
    }

    public TValue? GetByKey(ReadOnlySpan<byte> key)
    {
        return RocksDb.Get(key, ValueDeserializer, ColumnFamilyHandle);
    }

    public TValue? GetByKey(TKey key)
    {
        var keyBuffer = LocalBufferWriter.Value!;
        try
        {
            KeySerializer.Serialize(keyBuffer, key);
            return GetByKey(keyBuffer.WrittenSpan);
        }
        finally
        {
            keyBuffer.Dispose();
        }
    }

    public bool HasExactKey(TKey uniqueKey)
    {
        var keyBuffer = LocalBufferWriter.Value!;
        try
        {
            KeySerializer.Serialize(keyBuffer, uniqueKey);
            return RocksDb.HasKey(keyBuffer.WrittenSpan, ColumnFamilyHandle);
        }
        finally
        {
            keyBuffer.Dispose();
        }
    }

    public bool HasAnyKeyByPrefix<TPrefixKey>(TPrefixKey key, IRockSerializer<TPrefixKey> serializer)
    {
        var prefixBuffer = LocalBufferWriter.Value!;
        try
        {
            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle);
            serializer.Serialize(prefixBuffer, key);
            iterator.Seek(prefixBuffer.WrittenSpan);
            if (!iterator.Valid())
            {
                return false;
            }

            return iterator.GetKeySpan().StartsWith(prefixBuffer.WrittenSpan);
        }
        finally
        {
            prefixBuffer.Dispose();
        }
    }

    public IEnumerable<TKey> GetAllKeys()
    {
        using var iterator = RocksDb.NewIterator(ColumnFamilyHandle);
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
        {
            var fullKey = KeySerializer.Deserialize(iterator.GetKeySpan())!;
            yield return fullKey;
        }
    }

    public IEnumerable<TValue> GetAllValues()
    {
        using var iterator = RocksDb.NewIterator(ColumnFamilyHandle);
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
        {
            var value = ValueDeserializer.Deserialize(iterator.GetValueSpan())!;
            yield return value;
        }
    }

    public IEnumerable<TKey> GetAllKeysByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer)
    {
        var prefixBuffer = LocalBufferWriter.Value!;
        try
        {
            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle);
            prefixSerializer.Serialize(prefixBuffer, prefixKey);
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
            prefixBuffer.Dispose();
        }
    }

    public IEnumerable<TValue> GetAllValuesByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer)
    {
        var prefixBuffer = LocalBufferWriter.Value!;
        try
        {
            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle);
            prefixSerializer.Serialize(prefixBuffer, prefixKey);
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
            prefixBuffer.Dispose();
        }
    }

    public TValue? GetFirstValueByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> serializer, SeekMode mode = SeekMode.SeekToFirst)
    {
        var prefixBuffer = LocalBufferWriter.Value!;
        try
        {
            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle);
            serializer.Serialize(prefixBuffer, prefixKey);

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
            prefixBuffer.Dispose();
        }
    }

    public IEnumerable<TValue> GetAllValuesByBounds(TKey startInclusive, TKey endExclusive)
    {
        var buffer = LocalBufferWriter.Value!;
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
            buffer.Dispose();
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