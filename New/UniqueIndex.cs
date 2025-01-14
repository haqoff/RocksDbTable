using System;
using System.Collections.Generic;
using RocksDbSharp;

namespace Haqon.RocksDb.New;

internal class UniqueIndex<TUniqueKey, TPrimaryKey, TValue> : IUniqueIndex<TUniqueKey, TValue>, IDependentIndex<TValue>
{
    private readonly ColumnFamilyHandle _columnFamilyHandle;
    private readonly RocksDbSharp.RocksDb _rocksDb;
    private readonly Func<TValue, TUniqueKey> _keyProvider;
    private readonly IRockSerializer<TUniqueKey> _uniqueKeySerializer;
    private readonly IRockSerializer<TValue> _valueSerializer;
    private readonly IndexOptions _indexOptions;
    private readonly ISpanDeserializer<TValue> _valueDeserializer;

    public UniqueIndex(RocksDbSharp.RocksDb rocksDb, Func<TValue, TUniqueKey> keyProvider, IRockSerializer<TUniqueKey> uniqueKeySerializer, IRockSerializer<TValue> valueSerializer, IRocksDbTable<TPrimaryKey, TValue> table, IndexOptions indexOptions)
    {
        _rocksDb = rocksDb;
        _keyProvider = keyProvider;
        _uniqueKeySerializer = uniqueKeySerializer;
        _valueSerializer = valueSerializer;
        _indexOptions = indexOptions;
        _columnFamilyHandle = rocksDb.CreateColumnFamily(indexOptions.ColumnFamilyOptions, indexOptions.ColumnFamilyName);
        _valueDeserializer = _indexOptions.StoreMode == ValueStoreMode.FullValue
            ? new RocksDeserializerAdapter<TValue>(valueSerializer)
            : new ByReferenceToTableValueDeserializer<TPrimaryKey, TValue>(table);
    }

    public TValue? GetByKey(in TUniqueKey uniqueKey)
    {
        var keyBuffer = new ArrayPoolBufferWriter();
        try
        {
            _uniqueKeySerializer.Serialize(ref keyBuffer, in uniqueKey);
            return _rocksDb.Get(keyBuffer.WrittenSpan, _valueDeserializer, _columnFamilyHandle);
        }
        finally
        {
            keyBuffer.Dispose();
        }
    }

    public bool HasKey(in TUniqueKey uniqueKey)
    {
        var keyBuffer = new ArrayPoolBufferWriter();
        try
        {
            _uniqueKeySerializer.Serialize(ref keyBuffer, in uniqueKey);
            return _rocksDb.HasKey(keyBuffer.WrittenSpan, _columnFamilyHandle);
        }
        finally
        {
            keyBuffer.Dispose();
        }
    }

    public IEnumerable<TUniqueKey> GetAllKeys()
    {
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle);
            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            {
                var fullKey = _uniqueKeySerializer.Deserialize(iterator.GetKeySpan())!;
                yield return fullKey;
            }
        }
        finally
        {
            buffer.Dispose();
        }
    }

    public TValue? GetFirstValueByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> serializer, SeekMode mode = SeekMode.SeekToFirst)
    {
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            serializer.Serialize(ref buffer, in prefixKey);

            if (mode == SeekMode.SeekToPrev)
            {
                iterator.SeekForPrev(buffer.WrittenSpan);
            }
            else
            {
                iterator.Seek(buffer.WrittenSpan);
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
            buffer.Dispose();
        }
    }

    public IEnumerable<TUniqueKey> GetAllKeysByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer)
    {
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            prefixSerializer.Serialize(ref buffer, in prefixKey);
            for (iterator.Seek(buffer.WrittenSpan); iterator.Valid(); iterator.Next())
            {
                var fullKey = _uniqueKeySerializer.Deserialize(iterator.GetKeySpan())!;

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
            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            prefixSerializer.Serialize(ref buffer, in prefixKey);
            for (iterator.Seek(buffer.WrittenSpan); iterator.Valid(); iterator.Next())
            {
                yield return _valueDeserializer.Deserialize(iterator.GetValueSpan())!;
            }
        }
        finally
        {
            buffer.Dispose();
        }
    }

    public void Remove<TWrapper>(in ReadOnlySpan<byte> primaryKeySpan, TValue value, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        var bufferWriter = new ArrayPoolBufferWriter();
        try
        {
            var uniqueKey = _keyProvider(value);
            _uniqueKeySerializer.Serialize(ref bufferWriter, in uniqueKey);
            transaction.CommandWrapper.Delete(bufferWriter.WrittenSpan, _columnFamilyHandle);
        }
        finally
        {
            bufferWriter.Dispose();
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
                if (!EqualityComparer<TUniqueKey>.Default.Equals(newKey, oldKey))
                {
                    _uniqueKeySerializer.Serialize(ref keyBuffer, in oldKey);
                    transaction.CommandWrapper.Delete(keyBuffer.WrittenSpan, _columnFamilyHandle);
                    keyBuffer.Reset();
                    needAddReference = true;
                }
            }

            if (_indexOptions.StoreMode == ValueStoreMode.FullValue || (_indexOptions.StoreMode == ValueStoreMode.Reference && needAddReference))
            {
                _uniqueKeySerializer.Serialize(ref keyBuffer, in newKey);
                var serializedValue = _indexOptions.StoreMode == ValueStoreMode.FullValue ? valueSpan : primaryKeySpan;
                transaction.CommandWrapper.Put(keyBuffer.WrittenSpan, serializedValue, _columnFamilyHandle);
            }
        }
        finally
        {
            keyBuffer.Dispose();
        }
    }
}