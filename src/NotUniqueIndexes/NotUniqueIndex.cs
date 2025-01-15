using System;
using System.Collections.Generic;
using Haqon.RocksDb.Core;
using Haqon.RocksDb.Options;
using Haqon.RocksDb.Serialization;
using Haqon.RocksDb.Tables;
using Haqon.RocksDb.Transactions;
using Haqon.RocksDb.Utils;

namespace Haqon.RocksDb.NotUniqueIndexes;

internal class NotUniqueIndex<TNotUniqueKey, TValue> : KeyValueStoreBase<TNotUniqueKey, TValue>, INotUniqueIndex<TNotUniqueKey, TValue>, IDependentIndex<TValue>
{
    private readonly Func<TValue, TNotUniqueKey> _keyProvider;
    private readonly IndexOptions _indexOptions;

    public NotUniqueIndex(RocksDbSharp.RocksDb rocksDb, Func<TValue, TNotUniqueKey> keyProvider, IRockSerializer<TNotUniqueKey> notUniqueKeySerializer, IRockSerializer<TValue> valueSerializer, ITableValueProvider<TValue> table, IndexOptions indexOptions)
        : base(rocksDb, notUniqueKeySerializer, indexOptions, CreateSpanDeserializerForIndex(valueSerializer, table, indexOptions))
    {
        _keyProvider = keyProvider;
        _indexOptions = indexOptions;
    }

    public bool HasAny(in TNotUniqueKey key)
    {
        return HasAnyKeyByPrefix(in key, KeySerializer);
    }

    public TValue? GetFirstValue(in TNotUniqueKey key, SeekMode mode = SeekMode.SeekToFirst)
    {
        return GetFirstValueByPrefix(in key, KeySerializer, mode);
    }

    public IEnumerable<TValue> GetAllValuesByKey(TNotUniqueKey key)
    {
        return GetAllValuesByPrefix(key, KeySerializer);
    }

    public void Remove<TWrapper>(in ReadOnlySpan<byte> primaryKeySpan, TValue value, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        var key = _keyProvider(value);
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            KeySerializer.Serialize(ref buffer, in key);
            AppendSpanToBuffer(ref buffer, in primaryKeySpan);
            transaction.CommandWrapper.Delete(buffer.WrittenSpan, ColumnFamilyHandle);
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
                    KeySerializer.Serialize(ref keyBuffer, in oldKey);
                    AppendSpanToBuffer(ref keyBuffer, in primaryKeySpan);

                    transaction.CommandWrapper.Delete(keyBuffer.WrittenSpan, ColumnFamilyHandle);
                    keyBuffer.Reset();
                    needAddReference = true;
                }
            }

            if (_indexOptions.StoreMode == ValueStoreMode.FullValue || (_indexOptions.StoreMode == ValueStoreMode.Reference && needAddReference))
            {
                KeySerializer.Serialize(ref keyBuffer, in newKey);
                AppendSpanToBuffer(ref keyBuffer, in primaryKeySpan);

                // For Reference store mode: although we already store the primary key of the table in the key of this ColumnFamily, 
                // we still include the primary key in the record's value. This is because we cannot efficiently 
                // retrieve the primary key while maintaining the ability to perform flexible prefix searches.
                var serializedValue = _indexOptions.StoreMode == ValueStoreMode.FullValue ? valueSpan : primaryKeySpan;
                transaction.CommandWrapper.Put(keyBuffer.WrittenSpan, serializedValue, ColumnFamilyHandle);
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