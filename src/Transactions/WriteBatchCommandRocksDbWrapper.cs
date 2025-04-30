using System;
using RocksDbSharp;

namespace RocksDbTable.Transactions;

/// <summary>
/// Wrapper used to write operations to <see cref="WriteBatch"/>.
/// </summary>
/// <param name="batch">Batch of operations.</param>
public readonly struct WriteBatchCommandRocksDbWrapper(WriteBatch batch) : IRocksDbCommandWrapper
{
    /// <inheritdoc />
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ColumnFamilyHandle? cf = null)
    {
        batch.Put(key, value, cf);
    }

    /// <inheritdoc />
    public void Delete(ReadOnlySpan<byte> key, ColumnFamilyHandle? cf = null)
    {
        batch.Delete(key, cf);
    }
}