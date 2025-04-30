using System;
using RocksDbSharp;

namespace RocksDbTable.Transactions;

/// <summary>
/// Represents a command wrapper interface for performing low-level write operations.
/// </summary>
public interface IRocksDbCommandWrapper
{
    /// <summary>
    /// Writes the specified key-value pair to RocksDb, optionally to the specified column family.
    /// </summary>
    void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ColumnFamilyHandle? cf = null);

    /// <summary>
    /// Deletes the specified key from RocksDb, optionally from the specified column family.
    /// </summary>
    void Delete(ReadOnlySpan<byte> key, ColumnFamilyHandle? cf = null);
}