using System;
using RocksDbSharp;

namespace Haqon.RocksDb.Transactions;

public interface IRocksDbCommandWrapper
{
    void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ColumnFamilyHandle? cf = null);
    void Delete(ReadOnlySpan<byte> key, ColumnFamilyHandle? cf = null);
}