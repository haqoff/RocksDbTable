using System;
using System.Collections.Generic;
using RocksDbSharp;

namespace Haqon.RocksDb;

public static class ChangeTransaction
{
    public static ChangeTransaction<WriteBatchCommandRocksDbWrapper> CreateTransaction(this RocksDbSharp.RocksDb rocksDb, WriteOptions? writeOptions = null)
    {
        var batch = new WriteBatch();
        var wrapper = new WriteBatchCommandRocksDbWrapper(batch);
        return new ChangeTransaction<WriteBatchCommandRocksDbWrapper>(batch, wrapper, rocksDb, writeOptions);
    }

    internal static ChangeTransaction<RocksDbWrapper> CreateMockedTransaction(this RocksDbSharp.RocksDb rocksDb, WriteOptions? writeOptions = null)
    {
        var wrapper = new RocksDbWrapper(rocksDb, writeOptions);
        return new ChangeTransaction<RocksDbWrapper>(null, wrapper, rocksDb, writeOptions);
    }
}

public ref struct ChangeTransaction<TWrapper>
    where TWrapper : IRocksDbCommandWrapper
{
    private ArrayPoolBufferWriter _writer;
    private object? _lastSerializedValue;
    private readonly WriteBatch? _writeBatch;
    private readonly RocksDbSharp.RocksDb _rocksDb;
    private readonly WriteOptions? _writeOptions;
    private List<IChange>? _changes;

    internal ChangeTransaction(WriteBatch? writeBatch, TWrapper commandWrapper, RocksDbSharp.RocksDb rocksDb, WriteOptions? writeOptions)
    {
        _writer = new ArrayPoolBufferWriter();
        _writeBatch = writeBatch;
        _rocksDb = rocksDb;
        _writeOptions = writeOptions;
        CommandWrapper = commandWrapper;
    }

    internal TWrapper CommandWrapper { get; }

    internal void RegisterChange(IChange change)
    {
        (_changes ??= new List<IChange>(1)).Add(change);
    }

    internal ReadOnlySpan<byte> GetSerializedValue<TValue>(in TValue value, IRockSerializer<TValue> serializer)
    {
        var incomingType = typeof(TValue);
        if (_lastSerializedValue is null || _lastSerializedValue.GetType() != incomingType || !EqualityComparer<TValue>.Default.Equals((TValue)_lastSerializedValue, value))
        {
            if (!incomingType.IsValueType)
            {
                // escape boxing
                _lastSerializedValue = value;
            }

            _writer.Reset();
            serializer.Serialize(ref _writer, in value);
        }

        return _writer.WrittenSpan;
    }

    public void Commit()
    {
        if (_writeBatch is not null)
        {
            _rocksDb.Write(_writeBatch, _writeOptions);
        }

        if (_changes is not null)
        {
            foreach (IChange change in _changes)
            {
                change.Dispatch();
            }

            _changes = null;
        }
    }

    public void Dispose()
    {
        _writer.Dispose();
    }
}