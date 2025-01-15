using System;
using RocksDbSharp;

namespace Haqon.RocksDb.Transactions;

public readonly struct RocksDbWrapper(RocksDbSharp.RocksDb db, WriteOptions? writeOptions) : IRocksDbCommandWrapper
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