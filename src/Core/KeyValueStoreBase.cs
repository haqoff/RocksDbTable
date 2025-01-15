using System;
using System.Collections.Generic;
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
    protected readonly ColumnFamilyHandle ColumnFamilyHandle;
    protected readonly RocksDbSharp.RocksDb RocksDb;
    protected readonly IRockSerializer<TKey> KeySerializer;
    protected readonly ISpanDeserializer<TValue> ValueDeserializer;

    protected KeyValueStoreBase(RocksDbSharp.RocksDb rocksDb, IRockSerializer<TKey> keySerializer, IStoreOptions storeOptions, ISpanDeserializer<TValue> valueDeserializer)
    {
        RocksDb = rocksDb;
        KeySerializer = keySerializer;
        _options = storeOptions;
        ColumnFamilyHandle = rocksDb.CreateColumnFamily(storeOptions.ColumnFamilyOptions, storeOptions.ColumnFamilyName);
        ValueDeserializer = valueDeserializer;
    }

    
    public TValue? GetByKey(ReadOnlySpan<byte> key)
    {
        return RocksDb.Get(key, ValueDeserializer, ColumnFamilyHandle);
    }

    /// <summary>
    /// Retrieves a value associated with the specified key.
    /// </summary>
    /// <param name="key">The unique key.</param>
    /// <returns>The value associated with the unique key, or <c>null</c> if the key does not exist.</returns>
    public TValue? GetByKey(in TKey key)
    {
        var keyBuffer = new ArrayPoolBufferWriter();
        try
        {
            KeySerializer.Serialize(ref keyBuffer, in key);
            return GetByKey(keyBuffer.WrittenSpan);
        }
        finally
        {
            keyBuffer.Dispose();
        }
    }

    public bool HasExactKey(in TKey uniqueKey)
    {
        var keyBuffer = new ArrayPoolBufferWriter();
        try
        {
            KeySerializer.Serialize(ref keyBuffer, in uniqueKey);
            return RocksDb.HasKey(keyBuffer.WrittenSpan, ColumnFamilyHandle);
        }
        finally
        {
            keyBuffer.Dispose();
        }
    }

    public bool HasAnyKeyByPrefix<TPrefixKey>(in TPrefixKey key, IRockSerializer<TPrefixKey> serializer)
    {
        var notUniqueKeyBuffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            serializer.Serialize(ref notUniqueKeyBuffer, in key);
            iterator.Seek(notUniqueKeyBuffer.WrittenSpan);
            return iterator.Valid();
        }
        finally
        {
            notUniqueKeyBuffer.Dispose();
        }
    }

    public IEnumerable<TKey> GetAllKeys()
    {
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle);
            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            {
                var fullKey = KeySerializer.Deserialize(iterator.GetKeySpan())!;
                yield return fullKey;
            }
        }
        finally
        {
            buffer.Dispose();
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
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            prefixSerializer.Serialize(ref buffer, in prefixKey);
            for (iterator.Seek(buffer.WrittenSpan); iterator.Valid(); iterator.Next())
            {
                var fullKey = KeySerializer.Deserialize(iterator.GetKeySpan())!;
                yield return fullKey;
            }
        }
        finally
        {
            buffer.Dispose();
        }
    }

    public IEnumerable<TValue> GetAllValuesByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer)
    {
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            prefixSerializer.Serialize(ref buffer, in prefixKey);
            for (iterator.Seek(buffer.WrittenSpan); iterator.Valid(); iterator.Next())
            {
                yield return ValueDeserializer.Deserialize(iterator.GetValueSpan())!;
            }
        }
        finally
        {
            buffer.Dispose();
        }
    }

    public TValue? GetFirstValueByPrefix<TPrefixKey>(in TPrefixKey prefixKey, IRockSerializer<TPrefixKey> serializer, SeekMode mode = SeekMode.SeekToFirst)
    {
        var prefixBuffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            serializer.Serialize(ref prefixBuffer, in prefixKey);

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

            var value = ValueDeserializer.Deserialize(iterator.GetValueSpan());
            return value;
        }
        finally
        {
            prefixBuffer.Dispose();
        }
    }

    public IEnumerable<TValue> GetAllValuesByBounds(TKey start, TKey end)
    {
        var startBuffer = new ArrayPoolBufferWriter();
        var endBuffer = new ArrayPoolBufferWriter();

        try
        {
            KeySerializer.Serialize(ref startBuffer, start);
            KeySerializer.Serialize(ref endBuffer, end);

            var options = new ReadOptions();
            options.SetIterateLowerBound(startBuffer.GetUnderlyingArray(), (ulong)startBuffer.WrittenCount);
            options.SetIterateUpperBound(endBuffer.GetUnderlyingArray(), (ulong)endBuffer.WrittenCount);

            using var iterator = RocksDb.NewIterator(ColumnFamilyHandle, options);
            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            {
                yield return ValueDeserializer.Deserialize(iterator.GetValueSpan())!;
            }
        }
        finally
        {
            startBuffer.Dispose();
            endBuffer.Dispose();
        }
    }

    internal static ISpanDeserializer<TValue> CreateSpanDeserializerForIndex(IRockSerializer<TValue> valueSerializer, ITableValueProvider<TValue> table, IndexOptions indexOptions)
    {
        return indexOptions.StoreMode == ValueStoreMode.FullValue
            ? new RocksDbSpanDeserializerAdapter<TValue>(valueSerializer)
            : new ByReferenceToTableValueDeserializer<TValue>(table);
    }
}