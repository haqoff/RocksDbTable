using System;
using System.Collections.Generic;
using System.Threading;
using Haqon.RocksDb.ChangeTracking;
using Haqon.RocksDb.Core;
using Haqon.RocksDb.Extensions;
using Haqon.RocksDb.NotUniqueIndexes;
using Haqon.RocksDb.Options;
using Haqon.RocksDb.Serialization;
using Haqon.RocksDb.Transactions;
using Haqon.RocksDb.UniqueIndexes;
using Haqon.RocksDb.Utils;
using RocksDbSharp;

namespace Haqon.RocksDb.Tables;

internal class RocksDbTable<TPrimaryKey, TValue> : KeyValueStoreBase<TPrimaryKey, TValue>, IRocksDbTable<TPrimaryKey, TValue>, ITableValueProvider<TValue>
{
    private readonly Func<TValue, TPrimaryKey> _keyProvider;
    private readonly IRockSerializer<TValue> _valueSerializer;
    private readonly TableOptions<TPrimaryKey, TValue> _tableOptions;
    private readonly List<IDependentIndex<TValue>> _dependentIndexes = new();
    private readonly object[] _locks;

    internal RocksDbTable(RocksDbSharp.RocksDb rocksDb, Func<TValue, TPrimaryKey> keyProvider, IRockSerializer<TPrimaryKey> keySerializer, IRockSerializer<TValue> valueSerializer, TableOptions<TPrimaryKey, TValue> tableOptions)
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

    public void Remove(in TPrimaryKey primaryKey, WriteOptions? writeOptions = null)
    {
        if (_dependentIndexes.Count == 0)
        {
            var transaction = RocksDb.CreateMockedTransaction(writeOptions);
            try
            {
                Remove(in primaryKey, ref transaction);
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
                Remove(in primaryKey, ref transaction);
                transaction.Commit();
            }
            finally
            {
                transaction.Dispose();
            }
        }
    }

    public void Remove<TWrapper>(in TPrimaryKey primaryKey, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        var bufferWriter = new ArrayPoolBufferWriter();

        try
        {
            KeySerializer.Serialize(ref bufferWriter, in primaryKey);

            TValue? currentValue = default;
            var currentValueWasRetrieved = false;

            if (_dependentIndexes.Count > 0)
            {
                AcquireLockIfRequired(primaryKey, ref transaction);
                currentValue = RocksDb.Get(bufferWriter.WrittenSpan, ValueDeserializer, ColumnFamilyHandle);
                currentValueWasRetrieved = true;
                if (currentValue is null)
                {
                    return;
                }

                foreach (var dependentIndex in _dependentIndexes)
                {
                    dependentIndex.Remove(bufferWriter.WrittenSpan, currentValue, ref transaction);
                }
            }

            if (_tableOptions.ChangesConsumer is not null && !currentValueWasRetrieved)
            {
                AcquireLockIfRequired(primaryKey, ref transaction);
                currentValue = RocksDb.Get(bufferWriter.WrittenSpan, ValueDeserializer, ColumnFamilyHandle);
                if (currentValue is null)
                {
                    return;
                }
            }

            transaction.CommandWrapper.Delete(bufferWriter.WrittenSpan, ColumnFamilyHandle);

            if (_tableOptions.ChangesConsumer is not null)
            {
                transaction.RegisterChange(new RecordRemoved<TPrimaryKey, TValue>(primaryKey, currentValue!, _tableOptions.ChangesConsumer));
            }
        }
        finally
        {
            bufferWriter.Dispose();
        }
    }

    public void Put(in TValue newValue, WriteOptions? writeOptions = null)
    {
        if (_dependentIndexes.Count == 0)
        {
            var transaction = RocksDb.CreateMockedTransaction(writeOptions);
            try
            {
                Put(in newValue, ref transaction);
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
                Put(in newValue, ref transaction);
                transaction.Commit();
            }
            finally
            {
                transaction.Dispose();
            }
        }
    }

    public void Put<TWrapper>(in TValue newValue, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        var key = _keyProvider(newValue);

        var keyBufferWriter = new ArrayPoolBufferWriter();
        var valueBufferWriter = new ArrayPoolBufferWriter();
        try
        {
            KeySerializer.Serialize(ref keyBufferWriter, in key);
            _valueSerializer.Serialize(ref valueBufferWriter, in newValue);

            TValue? oldValue = default;
            var oldValueWasRetrieved = false;
            if (_dependentIndexes.Count > 0)
            {
                AcquireLockIfRequired(key, ref transaction);
                oldValue = RocksDb.Get(keyBufferWriter.WrittenSpan, ValueDeserializer, ColumnFamilyHandle);
                oldValueWasRetrieved = true;
                foreach (var dependentIndex in _dependentIndexes)
                {
                    dependentIndex.Put(keyBufferWriter.WrittenSpan, valueBufferWriter.WrittenSpan, oldValue, newValue, ref transaction);
                }
            }

            if (_tableOptions.ChangesConsumer is not null && !oldValueWasRetrieved)
            {
                AcquireLockIfRequired(key, ref transaction);
                oldValue = RocksDb.Get(keyBufferWriter.WrittenSpan, ValueDeserializer, ColumnFamilyHandle);
            }

            transaction.CommandWrapper.Put(keyBufferWriter.WrittenSpan, valueBufferWriter.WrittenSpan, ColumnFamilyHandle);
            if (_tableOptions.ChangesConsumer is not null)
            {
                transaction.RegisterChange(new RecordAddedOrUpdated<TPrimaryKey, TValue>(key, oldValue, newValue, _tableOptions.ChangesConsumer));
            }
        }
        finally
        {
            keyBufferWriter.Dispose();
            valueBufferWriter.Dispose();
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

    private void AcquireLockIfRequired<TWrapper>(in TPrimaryKey primaryKey, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        if (!_tableOptions.EnableConcurrentChangesWithinRow)
        {
            return;
        }

        var lockIndex = (primaryKey is null ? 0 : primaryKey.GetHashCode()) % _locks.Length;
        var lockObject = _locks[lockIndex];
        Monitor.Enter(lockObject);
        transaction.AddTakenLock(lockObject);
    }
}