using System;
using System.Collections.Generic;
using RocksDbTable.Core;
using RocksDbTable.Options;
using RocksDbTable.Serialization;
using RocksDbTable.Tables;
using RocksDbTable.Transactions;

namespace RocksDbTable.UniqueIndexes;

internal sealed class UniqueIndex<TUniqueKey, TValue> : KeyValueStoreBase<TUniqueKey, TValue>, IUniqueIndex<TUniqueKey, TValue>, IDependentIndex<TValue>
{
    private readonly Func<TValue, TUniqueKey> _keyProvider;
    private readonly IndexOptions _indexOptions;

    internal UniqueIndex(RocksDbSharp.RocksDb rocksDb, Func<TValue, TUniqueKey> keyProvider, IRockSerializer<TUniqueKey> uniqueKeySerializer, IRockSerializer<TValue> valueSerializer, ITableValueProvider<TValue> table, IndexOptions indexOptions)
        : base(rocksDb, uniqueKeySerializer, indexOptions, CreateSpanDeserializerForIndex(valueSerializer, table, indexOptions))
    {
        _keyProvider = keyProvider;
        _indexOptions = indexOptions;
    }

    public void Remove<TWrapper>(ReadOnlySpan<byte> primaryKeySpan, TValue value, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        var bufferWriter = RentBufferWriter();
        try
        {
            var uniqueKey = _keyProvider(value);
            KeySerializer.Serialize(bufferWriter, uniqueKey);
            transaction.CommandWrapper.Delete(bufferWriter.WrittenSpan, ColumnFamilyHandle);
        }
        finally
        {
            ReturnBufferWriter(bufferWriter);
        }
    }

    public void Put<TWrapper>(ReadOnlySpan<byte> primaryKeySpan, ReadOnlySpan<byte> valueSpan, TValue? oldValue, TValue newValue, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        var newKey = _keyProvider(newValue);
        var keyBuffer = RentBufferWriter();
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
                    KeySerializer.Serialize(keyBuffer, oldKey);
                    transaction.CommandWrapper.Delete(keyBuffer.WrittenSpan, ColumnFamilyHandle);
                    keyBuffer.Reset();
                    needAddReference = true;
                }
            }

            if (_indexOptions.StoreMode == ValueStoreMode.FullValue || (_indexOptions.StoreMode == ValueStoreMode.Reference && needAddReference))
            {
                KeySerializer.Serialize(keyBuffer, newKey);
                var serializedValue = _indexOptions.StoreMode == ValueStoreMode.FullValue ? valueSpan : primaryKeySpan;
                transaction.CommandWrapper.Put(keyBuffer.WrittenSpan, serializedValue, ColumnFamilyHandle);
            }
        }
        finally
        {
            ReturnBufferWriter(keyBuffer);
        }
    }
}