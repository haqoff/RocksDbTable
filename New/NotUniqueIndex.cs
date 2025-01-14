using System;
using System.Collections.Generic;
using RocksDbSharp;

namespace Haqon.RocksDb.New;

public class NotUniqueIndex<TNotUniqueKey, TPrimaryKey, TValue> : INotUniqueIndex<TNotUniqueKey, TValue>, IDependentIndex<TValue>
{
    private readonly ColumnFamilyHandle _columnFamilyHandle;
    private readonly RocksDbSharp.RocksDb _rocksDb;
    private readonly Func<TValue, TNotUniqueKey> _keyProvider;
    private readonly IRockSerializer<TNotUniqueKey> _notUniqueKeySerializer;
    private readonly IRockSerializer<TValue> _valueSerializer;
    private readonly IndexOptions _indexOptions;
    private readonly ISpanDeserializer<TValue> _valueDeserializer;

    public NotUniqueIndex(RocksDbSharp.RocksDb rocksDb, Func<TValue, TNotUniqueKey> keyProvider, IRockSerializer<TNotUniqueKey> notUniqueKeySerializer, IRockSerializer<TValue> valueSerializer, IRocksDbTable<TPrimaryKey, TValue> table, IndexOptions indexOptions)
    {
        _rocksDb = rocksDb;
        _keyProvider = keyProvider;
        _notUniqueKeySerializer = notUniqueKeySerializer;
        _valueSerializer = valueSerializer;
        _indexOptions = indexOptions;
        _columnFamilyHandle = rocksDb.CreateColumnFamily(indexOptions.ColumnFamilyOptions, indexOptions.ColumnFamilyName);
        _valueDeserializer = _indexOptions.StoreMode == ValueStoreMode.FullValue
            ? new RocksDeserializerAdapter<TValue>(valueSerializer)
            : new ByReferenceToTableValueDeserializer<TPrimaryKey, TValue>(table);
    }

    public bool HasAny(in TNotUniqueKey key)
    {
        var notUniqueKeyBuffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            _notUniqueKeySerializer.Serialize(ref notUniqueKeyBuffer, in key);
            iterator.Seek(notUniqueKeyBuffer.WrittenSpan);
            return iterator.Valid();
        }
        finally
        {
            notUniqueKeyBuffer.Dispose();
        }
    }

    public TValue? GetFirstValue(in TNotUniqueKey key, SeekMode mode = SeekMode.SeekToFirst)
    {
        var notUniqueKeyBuffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            _notUniqueKeySerializer.Serialize(ref notUniqueKeyBuffer, in key);

            if (mode == SeekMode.SeekToPrev)
            {
                iterator.SeekForPrev(notUniqueKeyBuffer.WrittenSpan);
            }
            else
            {
                iterator.Seek(notUniqueKeyBuffer.WrittenSpan);
            }

            if (!iterator.Valid())
            {
                return default;
            }

            var value = _valueDeserializer.Deserialize(iterator.GetValueSpan());
            return value;
        }
        finally
        {
            notUniqueKeyBuffer.Dispose();
        }
    }

    public TValue? GetFirstValueByPrefix<TPrefixKey>(in TPrefixKey prefixKey, IRockSerializer<TPrefixKey> serializer, SeekMode mode = SeekMode.SeekToFirst)
    {
        var prefixBuffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
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

            var value = _valueDeserializer.Deserialize(iterator.GetValueSpan());
            return value;
        }
        finally
        {
            prefixBuffer.Dispose();
        }
    }

    public IEnumerable<TValue> GetAllValuesByKey(TNotUniqueKey key)
    {
        var notUniqueKeyBuffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            _notUniqueKeySerializer.Serialize(ref notUniqueKeyBuffer, in key);
            for (iterator.Seek(notUniqueKeyBuffer.WrittenSpan); iterator.Valid(); iterator.Next())
            {
                yield return _valueDeserializer.Deserialize(iterator.GetValueSpan())!;
            }
        }
        finally
        {
            notUniqueKeyBuffer.Dispose();
        }
    }

    public IEnumerable<TValue> GetAllValuesByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer)
    {
        var prefixBuffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            prefixSerializer.Serialize(ref prefixBuffer, in prefixKey);
            for (iterator.Seek(prefixBuffer.WrittenSpan); iterator.Valid(); iterator.Next())
            {
                yield return _valueDeserializer.Deserialize(iterator.GetValueSpan())!;
            }
        }
        finally
        {
            prefixBuffer.Dispose();
        }
    }

    public IEnumerable<TValue> GetAllValuesByBounds(TNotUniqueKey start, TNotUniqueKey end)
    {
        var startBuffer = new ArrayPoolBufferWriter();
        var endBuffer = new ArrayPoolBufferWriter();

        try
        {
            _notUniqueKeySerializer.Serialize(ref startBuffer, start);
            _notUniqueKeySerializer.Serialize(ref endBuffer, end);

            var options = new ReadOptions();
            options.SetIterateLowerBound(startBuffer.GetUnderlyingArray(), (ulong)startBuffer.WrittenCount);
            options.SetIterateUpperBound(endBuffer.GetUnderlyingArray(), (ulong)endBuffer.WrittenCount);

            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle, options);
            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            {
                yield return _valueDeserializer.Deserialize(iterator.GetValueSpan())!;
            }
        }
        finally
        {
            startBuffer.Dispose();
            endBuffer.Dispose();
        }
    }

    public void Remove<TWrapper>(in ReadOnlySpan<byte> primaryKeySpan, TValue value, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        var key = _keyProvider(value);
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            _notUniqueKeySerializer.Serialize(ref buffer, in key);
            AppendSpanToBuffer(ref buffer, in primaryKeySpan);
            transaction.CommandWrapper.Delete(buffer.WrittenSpan, _columnFamilyHandle);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    public void Put<TWrapper>(in ReadOnlySpan<byte> primaryKeySpan, in ReadOnlySpan<byte> valueSpan, TValue? oldValue, TValue newValue, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        var newKey = _keyProvider(newValue);
        var keyBuffer = new ArrayPoolBufferWriter();
        try
        {
            var needAddReference = false;
            if (oldValue is null)
            {
                needAddReference = true;
            }
            else
            {
                var oldKey = _keyProvider(oldValue);
                if (!EqualityComparer<TNotUniqueKey>.Default.Equals(newKey, oldKey))
                {
                    _notUniqueKeySerializer.Serialize(ref keyBuffer, in oldKey);
                    AppendSpanToBuffer(ref keyBuffer, in primaryKeySpan);

                    transaction.CommandWrapper.Delete(keyBuffer.WrittenSpan, _columnFamilyHandle);
                    keyBuffer.Reset();
                    needAddReference = true;
                }
            }

            if (_indexOptions.StoreMode == ValueStoreMode.FullValue || (_indexOptions.StoreMode == ValueStoreMode.Reference && needAddReference))
            {
                _notUniqueKeySerializer.Serialize(ref keyBuffer, in newKey);
                AppendSpanToBuffer(ref keyBuffer, in primaryKeySpan);

                // For Reference store mode: although we already store the primary key of the table in the key of this ColumnFamily, 
                // we still include the primary key in the record's value. This is because we cannot efficiently 
                // retrieve the primary key while maintaining the ability to perform flexible prefix searches.
                var serializedValue = _indexOptions.StoreMode == ValueStoreMode.FullValue ? valueSpan : primaryKeySpan;
                transaction.CommandWrapper.Put(keyBuffer.WrittenSpan, serializedValue, _columnFamilyHandle);
            }
        }
        finally
        {
            keyBuffer.Dispose();
        }
    }

    private static void AppendSpanToBuffer(ref ArrayPoolBufferWriter buffer, in ReadOnlySpan<byte> appendingSpan)
    {
        var writeSpan = buffer.GetSpan(appendingSpan.Length);
        appendingSpan.CopyTo(writeSpan);
        buffer.Advance(appendingSpan.Length);
    }
}