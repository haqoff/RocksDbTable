using System;
using RocksDbSharp;

namespace RocksDbTable.Transactions;

/// <summary>
/// Wrapper used to write directly to <see cref="RocksDb"/>.
/// </summary>
/// <param name="db">DB.</param>
/// <param name="writeOptions">Write options.</param>
public readonly struct RocksDbWrapper(RocksDb db, WriteOptions? writeOptions) : IRocksDbCommandWrapper
{
    /// <inheritdoc />
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ColumnFamilyHandle? cf = null)
    {
        db.Put(key, value, cf, writeOptions);
    }

    /// <inheritdoc />
    public void Delete(ReadOnlySpan<byte> key, ColumnFamilyHandle? cf = null)
    {
        db.Remove(key, cf, writeOptions);
    }
}