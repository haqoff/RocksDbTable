using System;
using System.Collections.Generic;
using Haqon.RocksDb.Core;
using Haqon.RocksDb.Options;
using Haqon.RocksDb.Serialization;
using Haqon.RocksDb.Tables;
using Haqon.RocksDb.Transactions;
using Haqon.RocksDb.Utils;

namespace Haqon.RocksDb.UniqueIndexes;

internal class UniqueIndex<TUniqueKey, TValue> : KeyValueStoreBase<TUniqueKey, TValue>, IUniqueIndex<TUniqueKey, TValue>, IDependentIndex<TValue>
{
    private readonly Func<TValue, TUniqueKey> _keyProvider;
    private readonly IndexOptions _indexOptions;

    internal UniqueIndex(RocksDbSharp.RocksDb rocksDb, Func<TValue, TUniqueKey> keyProvider, IRockSerializer<TUniqueKey> uniqueKeySerializer, IRockSerializer<TValue> valueSerializer, ITableValueProvider<TValue> table, IndexOptions indexOptions)
        : base(rocksDb, uniqueKeySerializer, indexOptions, CreateSpanDeserializerForIndex(valueSerializer, table, indexOptions))
    {
        _keyProvider = keyProvider;
        _indexOptions = indexOptions;
    }

    public void Remove<TWrapper>(in ReadOnlySpan<byte> primaryKeySpan, TValue value, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        var bufferWriter = new ArrayPoolBufferWriter();
        try
        {
            var uniqueKey = _keyProvider(value);
            KeySerializer.Serialize(ref bufferWriter, in uniqueKey);
            transaction.CommandWrapper.Delete(bufferWriter.WrittenSpan, ColumnFamilyHandle);
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
                    KeySerializer.Serialize(ref keyBuffer, in oldKey);
                    transaction.CommandWrapper.Delete(keyBuffer.WrittenSpan, ColumnFamilyHandle);
                    keyBuffer.Reset();
                    needAddReference = true;
                }
            }

            if (_indexOptions.StoreMode == ValueStoreMode.FullValue || (_indexOptions.StoreMode == ValueStoreMode.Reference && needAddReference))
            {
                KeySerializer.Serialize(ref keyBuffer, in newKey);
                var serializedValue = _indexOptions.StoreMode == ValueStoreMode.FullValue ? valueSpan : primaryKeySpan;
                transaction.CommandWrapper.Put(keyBuffer.WrittenSpan, serializedValue, ColumnFamilyHandle);
            }
        }
        finally
        {
            keyBuffer.Dispose();
        }
    }
}