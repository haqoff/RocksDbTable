using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Haqon.RocksDb.ChangeTracking;
using RocksDbSharp;

namespace Haqon.RocksDb.Transactions;

public ref struct ChangeTransaction<TWrapper>
    where TWrapper : IRocksDbCommandWrapper
{
    private readonly WriteBatch? _writeBatch;
    private readonly RocksDbSharp.RocksDb _rocksDb;
    private readonly WriteOptions? _writeOptions;
    private List<ITableChange>? _changes;

    private object? _singleLock = null;
    private HashSet<object>? _multipleLocks = null;

    internal int TakenLockCount => _singleLock is not null ? 1 : _multipleLocks?.Count ?? 0;

    internal ChangeTransaction(WriteBatch? writeBatch, TWrapper commandWrapper, RocksDbSharp.RocksDb rocksDb, WriteOptions? writeOptions)
    {
        _writeBatch = writeBatch;
        _rocksDb = rocksDb;
        _writeOptions = writeOptions;
        CommandWrapper = commandWrapper;
    }

    internal TWrapper CommandWrapper { get; }

    internal void RegisterChange(ITableChange tableChange)
    {
        (_changes ??= new List<ITableChange>(4)).Add(tableChange);
    }

    public void Commit()
    {
        if (_writeBatch is not null)
        {
            _rocksDb.Write(_writeBatch, _writeOptions);
        }

        if (_changes is not null)
        {
            foreach (var change in _changes)
            {
                change.Dispatch();
            }

            _changes = null;
        }

        ExitLocks();
    }

    public void Dispose()
    {
        _changes = null;
        ExitLocks();
    }

    internal bool HasLock(object lockObject)
    {
        if (_singleLock is not null)
        {
            return _singleLock == lockObject;
        }

        if (_multipleLocks is not null)
        {
            return _multipleLocks.Contains(lockObject);
        }

        return false;
    }

    internal void AddTakenLock(object lockObject)
    {
        if (_singleLock is null && _multipleLocks is null)
        {
            _singleLock = lockObject;
        }
        else if (_singleLock is not null && _multipleLocks is null)
        {
            _multipleLocks = new HashSet<object>(8);
            _multipleLocks.Add(_singleLock);
            _multipleLocks.Add(lockObject);
            _singleLock = null;
        }
        else if (_singleLock is null && _multipleLocks is not null)
        {
            _multipleLocks.Add(lockObject);
        }
        else
        {
            Debug.Assert(false, "Unexpected!");
        }
    }

    private void ExitLocks()
    {
        if (_singleLock is not null)
        {
            Monitor.Exit(_singleLock);
            _singleLock = null;
        }

        if (_multipleLocks is not null)
        {
            foreach (var multipleLock in _multipleLocks)
            {
                Monitor.Exit(multipleLock);
            }

            _multipleLocks = null;
        }
    }
}