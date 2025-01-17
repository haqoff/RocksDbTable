using System;
using RocksDbSharp;

namespace RocksDbTable.Transactions;

public readonly struct RocksDbWrapper(RocksDb db, WriteOptions? writeOptions) : IRocksDbCommandWrapper
{
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ColumnFamilyHandle? cf = null)
    {
        db.Put(key, value, cf, writeOptions);
    }

    public void Delete(ReadOnlySpan<byte> key, ColumnFamilyHandle? cf = null)
    {
        db.Remove(key, cf, writeOptions);
    }
}