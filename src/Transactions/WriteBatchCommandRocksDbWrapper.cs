using System;
using RocksDbSharp;

namespace Haqon.RocksDb.Transactions;

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