using System;
using RocksDbSharp;

namespace Haqon.RocksDb;

public interface IRocksDbCommandWrapper
{
    void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ColumnFamilyHandle? cf = null);
    void Delete(ReadOnlySpan<byte> key, ColumnFamilyHandle? cf = null);
}

public readonly struct WriteBatchCommandRocksDbWrapper(WriteBatch batch) : IRocksDbCommandWrapper
{
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ColumnFamilyHandle? cf = null)
    {
        batch.Put(key, value, cf);
    }

    public void Delete(ReadOnlySpan<byte> key, ColumnFamilyHandle? cf = null)
    {
        batch.Delete(key, cf);
    }
}

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