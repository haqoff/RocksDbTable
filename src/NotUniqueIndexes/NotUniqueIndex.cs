using System;
using System.Collections.Generic;
using RocksDbTable.Core;
using RocksDbTable.Options;
using RocksDbTable.Serialization;
using RocksDbTable.Tables;
using RocksDbTable.Tracing;
using RocksDbTable.Transactions;
using RocksDbTable.Utils;

namespace RocksDbTable.NotUniqueIndexes;

internal sealed class NotUniqueIndex<TNotUniqueKey, TValue> : KeyValueStoreBase<TNotUniqueKey, TValue>, INotUniqueIndex<TNotUniqueKey, TValue>, IDependentIndex<TValue>
{
    private readonly Func<TValue, TNotUniqueKey> _keyProvider;
    private readonly IndexOptions _indexOptions;

    public NotUniqueIndex(RocksDbSharp.RocksDb rocksDb, Func<TValue, TNotUniqueKey> keyProvider, IRockSerializer<TNotUniqueKey> notUniqueKeySerializer, IRockSerializer<TValue> valueSerializer, ITableValueProvider<TValue> table, IndexOptions indexOptions)
        : base(rocksDb, notUniqueKeySerializer, indexOptions, CreateSpanDeserializerForIndex(valueSerializer, table, indexOptions))
    {
        _keyProvider = keyProvider;
        _indexOptions = indexOptions;
    }

    public bool HasAny(TNotUniqueKey key)
    {
        return HasAnyKeyByPrefix(key, KeySerializer);
    }

    public TValue? GetFirstValue(TNotUniqueKey key, SeekMode mode = SeekMode.SeekToFirst)
    {
        return GetFirstValueByPrefix(key, KeySerializer, mode);
    }

    public IEnumerable<TValue> GetAllValuesByKey(TNotUniqueKey key)
    {
        return GetAllValuesByPrefix(key, KeySerializer);
    }

    public void Remove<TWrapper>(ReadOnlySpan<byte> primaryKeySpan, TValue value, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        using var activity = StartActivity(ActivityNames.NotUniqueIndexRemove);
        var key = _keyProvider(value);
        var buffer = RentBufferWriter();
        try
        {
            KeySerializer.Serialize(buffer, key);
            AppendSpanToBuffer(buffer, primaryKeySpan);
            activity.SetKeyToActivity(buffer.WrittenSpan);
            transaction.CommandWrapper.Delete(buffer.WrittenSpan, ColumnFamilyHandle);
        }
        finally
        {
            ReturnBufferWriter(buffer);
        }
    }

    public void Put<TWrapper>(ReadOnlySpan<byte> primaryKeySpan, ReadOnlySpan<byte> valueSpan, TValue? oldValue, TValue newValue, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        using var activity = StartActivity(ActivityNames.NotUniqueIndexPut);
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
                if (!EqualityComparer<TNotUniqueKey>.Default.Equals(newKey, oldKey))
                {
                    using var removeOldKeyActivity = StartActivity(ActivityNames.NotUniqueIndexPutRemoveOldKey);
                    KeySerializer.Serialize(keyBuffer, oldKey);
                    AppendSpanToBuffer(keyBuffer, primaryKeySpan);
                    removeOldKeyActivity.SetKeyToActivity(keyBuffer.WrittenSpan);

                    transaction.CommandWrapper.Delete(keyBuffer.WrittenSpan, ColumnFamilyHandle);
                    keyBuffer.Reset();
                    needAddReference = true;
                }
            }

            if (_indexOptions.StoreMode == ValueStoreMode.FullValue || (_indexOptions.StoreMode == ValueStoreMode.Reference && needAddReference))
            {
                KeySerializer.Serialize(keyBuffer, newKey);
                AppendSpanToBuffer(keyBuffer, primaryKeySpan);
                activity.SetKeyToActivity(keyBuffer.WrittenSpan);

                // For Reference store mode: although we already store the primary key of the table in the key of this ColumnFamily, 
                // we still include the primary key in the record's value. This is because we cannot efficiently 
                // retrieve the primary key while maintaining the ability to perform flexible prefix searches.
                var serializedValue = _indexOptions.StoreMode == ValueStoreMode.FullValue ? valueSpan : primaryKeySpan;
                transaction.CommandWrapper.Put(keyBuffer.WrittenSpan, serializedValue, ColumnFamilyHandle);
            }
        }
        finally
        {
            ReturnBufferWriter(keyBuffer);
        }
    }

    private static void AppendSpanToBuffer(ArrayPoolBufferWriter buffer, ReadOnlySpan<byte> appendingSpan)
    {
        var writeSpan = buffer.GetSpan(appendingSpan.Length);
        appendingSpan.CopyTo(writeSpan);
        buffer.Advance(appendingSpan.Length);
    }
}