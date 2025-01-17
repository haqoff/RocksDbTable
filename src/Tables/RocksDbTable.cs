using System;
using System.Collections.Generic;
using System.Threading;
using RocksDbSharp;
using RocksDbTable.ChangeTracking;
using RocksDbTable.Core;
using RocksDbTable.Extensions;
using RocksDbTable.NotUniqueIndexes;
using RocksDbTable.Options;
using RocksDbTable.Serialization;
using RocksDbTable.Transactions;
using RocksDbTable.UniqueIndexes;

namespace RocksDbTable.Tables;

internal sealed class RocksDbTable<TPrimaryKey, TValue> : KeyValueStoreBase<TPrimaryKey, TValue>, IRocksDbTable<TPrimaryKey, TValue>, ITableValueProvider<TValue>
{
    private readonly Func<TValue, TPrimaryKey> _keyProvider;
    private readonly IRockSerializer<TValue> _valueSerializer;
    private readonly TableOptions<TPrimaryKey, TValue> _tableOptions;
    private readonly List<IDependentIndex<TValue>> _dependentIndexes = new();
    private readonly object[] _locks;

    internal RocksDbTable(RocksDb rocksDb, Func<TValue, TPrimaryKey> keyProvider, IRockSerializer<TPrimaryKey> keySerializer, IRockSerializer<TValue> valueSerializer, TableOptions<TPrimaryKey, TValue> tableOptions)
        : base(rocksDb, keySerializer, tableOptions, new RocksDbSpanDeserializerAdapter<TValue>(valueSerializer))
    {
        _keyProvider = keyProvider;
        _valueSerializer = valueSerializer;
        _tableOptions = tableOptions;

        if (tableOptions.EnableConcurrentChangesWithinRow)
        {
            _locks = new object[tableOptions.LockCount];
            for (int i = 0; i < _locks.Length; i++)
            {
                _locks[i] = new();
            }
        }
        else
        {
            _locks = [];
        }
    }

    public void Remove(TPrimaryKey primaryKey, WriteOptions? writeOptions = null)
    {
        if (_dependentIndexes.Count == 0)
        {
            var transaction = RocksDb.CreateMockedTransaction(writeOptions);
            try
            {
                Remove(primaryKey, ref transaction);
                transaction.Commit();
            }
            finally
            {
                transaction.Dispose();
            }
        }
        else
        {
            var transaction = RocksDb.CreateTransaction(writeOptions);
            try
            {
                Remove(primaryKey, ref transaction);
                transaction.Commit();
            }
            finally
            {
                transaction.Dispose();
            }
        }
    }

    public void Remove<TWrapper>(TPrimaryKey primaryKey, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        var buffer = LocalBufferWriter.Value!;
        try
        {
            KeySerializer.Serialize(buffer, primaryKey);

            TValue? currentValue = default;
            var currentValueWasRetrieved = false;

            if (_dependentIndexes.Count > 0)
            {
                AcquireLockIfRequired(primaryKey, ref transaction);
                currentValue = RocksDb.Get(buffer.WrittenSpan, ValueDeserializer, ColumnFamilyHandle);
                currentValueWasRetrieved = true;
                if (currentValue is null)
                {
                    return;
                }

                foreach (var dependentIndex in _dependentIndexes)
                {
                    dependentIndex.Remove(buffer.WrittenSpan, currentValue, ref transaction);
                }
            }

            if (_tableOptions.ChangesConsumer is not null && !currentValueWasRetrieved)
            {
                AcquireLockIfRequired(primaryKey, ref transaction);
                currentValue = RocksDb.Get(buffer.WrittenSpan, ValueDeserializer, ColumnFamilyHandle);
                if (currentValue is null)
                {
                    return;
                }
            }

            transaction.CommandWrapper.Delete(buffer.WrittenSpan, ColumnFamilyHandle);

            if (_tableOptions.ChangesConsumer is not null)
            {
                transaction.RegisterChange(new RecordRemoved<TPrimaryKey, TValue>(primaryKey, currentValue!, _tableOptions.ChangesConsumer));
            }
        }
        finally
        {
            buffer.Dispose();
        }
    }

    public void Put(TValue newValue, WriteOptions? writeOptions = null)
    {
        if (_dependentIndexes.Count == 0)
        {
            var transaction = RocksDb.CreateMockedTransaction(writeOptions);
            try
            {
                Put(newValue, ref transaction);
                transaction.Commit();
            }
            finally
            {
                transaction.Dispose();
            }
        }
        else
        {
            var transaction = RocksDb.CreateTransaction(writeOptions);
            try
            {
                Put(newValue, ref transaction);
                transaction.Commit();
            }
            finally
            {
                transaction.Dispose();
            }
        }
    }

    public void Put<TWrapper>(TValue newValue, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        var key = _keyProvider(newValue);
        var buffer = LocalBufferWriter.Value!;
        try
        {
            KeySerializer.Serialize(buffer, key);
            var keyPoint = new SpanPoint(buffer);
            _valueSerializer.Serialize(buffer, newValue);
            var valuePoint = new SpanPoint(buffer, keyPoint);

            var keySpan = keyPoint.GetWrittenSpan(buffer);
            var valueSpan = valuePoint.GetWrittenSpan(buffer);

            TValue? oldValue = default;
            var oldValueWasRetrieved = false;
            if (_dependentIndexes.Count > 0)
            {
                AcquireLockIfRequired(key, ref transaction);
                oldValue = RocksDb.Get(keySpan, ValueDeserializer, ColumnFamilyHandle);
                oldValueWasRetrieved = true;
                foreach (var dependentIndex in _dependentIndexes)
                {
                    dependentIndex.Put(keySpan, valueSpan, oldValue, newValue, ref transaction);
                }
            }

            if (_tableOptions.ChangesConsumer is not null && !oldValueWasRetrieved)
            {
                AcquireLockIfRequired(key, ref transaction);
                oldValue = RocksDb.Get(keySpan, ValueDeserializer, ColumnFamilyHandle);
            }

            transaction.CommandWrapper.Put(keySpan, valueSpan, ColumnFamilyHandle);
            if (_tableOptions.ChangesConsumer is not null)
            {
                transaction.RegisterChange(new RecordAddedOrUpdated<TPrimaryKey, TValue>(key, oldValue, newValue, _tableOptions.ChangesConsumer));
            }
        }
        finally
        {
            buffer.Dispose();
        }
    }

    public IUniqueIndex<TUniqueKey, TValue> CreateUniqueIndex<TUniqueKey>(Func<TValue, TUniqueKey> uniqueKeyProvider, IRockSerializer<TUniqueKey> keySerializer, Action<IndexOptions>? configureAction = null)
    {
        var indexOptions = new IndexOptions();
        configureAction?.Invoke(indexOptions);
        var index = new UniqueIndex<TUniqueKey, TValue>(RocksDb, uniqueKeyProvider, keySerializer, _valueSerializer, this, indexOptions);
        _dependentIndexes.Add(index);
        return index;
    }

    public INotUniqueIndex<TNotUniqueKey, TValue> CreateNotUniqueIndex<TNotUniqueKey>(Func<TValue, TNotUniqueKey> notUniqueKeyProvider, IRockSerializer<TNotUniqueKey> keySerializer, Action<IndexOptions>? configureAction = null)
    {
        var indexOptions = new IndexOptions();
        configureAction?.Invoke(indexOptions);
        var index = new NotUniqueIndex<TNotUniqueKey, TValue>(RocksDb, notUniqueKeyProvider, keySerializer, _valueSerializer, this, indexOptions);
        _dependentIndexes.Add(index);
        return index;
    }

    private void AcquireLockIfRequired<TWrapper>(TPrimaryKey primaryKey, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        if (!_tableOptions.EnableConcurrentChangesWithinRow)
        {
            return;
        }

        var lockIndex = (primaryKey is null ? 0 : primaryKey.GetHashCode()) % _locks.Length;
        var lockObject = _locks[lockIndex];
        if (transaction.HasLock(lockObject))
        {
            return;
        }

        Monitor.Enter(lockObject);
        transaction.AddTakenLock(lockObject);
    }
}