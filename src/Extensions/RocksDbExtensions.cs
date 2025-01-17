using System;
using RocksDbSharp;
using RocksDbTable.Options;
using RocksDbTable.Serialization;
using RocksDbTable.Tables;
using RocksDbTable.Transactions;

namespace RocksDbTable.Extensions;

/// <summary>
/// Provides extension methods for <see cref="RocksDb"/>.
/// </summary>
public static class RocksDbExtensions
{
    /// <summary>
    /// Creates a transaction within which changes will be applied atomically to the database.
    /// </summary>
    /// <param name="rocksDb">The RocksDb database instance.</param>
    /// <param name="writeOptions">Optional write options.</param>
    /// <returns>A transaction that can be used for operations in tables.</returns>
    public static ChangeTransaction<WriteBatchCommandRocksDbWrapper> CreateTransaction(this RocksDb rocksDb, WriteOptions? writeOptions = null)
    {
        var batch = new WriteBatch();
        var wrapper = new WriteBatchCommandRocksDbWrapper(batch);
        return new ChangeTransaction<WriteBatchCommandRocksDbWrapper>(batch, wrapper, rocksDb, writeOptions);
    }

    /// <summary>
    /// Creates a table in the RocksDb database.
    /// </summary>
    /// <typeparam name="TPrimaryKey">The type of the primary key.</typeparam>
    /// <typeparam name="TValue">The type of the table values.</typeparam>
    /// <param name="rocksDb">The RocksDb database instance.</param>
    /// <param name="primaryKeyProvider">A function to provide the primary key from a value.</param>
    /// <param name="primaryKeySerializer">Serializer for the primary key.</param>
    /// <param name="valueSerializer">Serializer for the table values.</param>
    /// <param name="configureAction">Optional configuration action for table options.</param>
    /// <returns>An instance of <see cref="IRocksDbTable{TPrimaryKey, TValue}"/> representing the created table.</returns>
    public static IRocksDbTable<TPrimaryKey, TValue> CreateTable<TPrimaryKey, TValue>(this RocksDb rocksDb, Func<TValue, TPrimaryKey> primaryKeyProvider, IRockSerializer<TPrimaryKey> primaryKeySerializer, IRockSerializer<TValue> valueSerializer, Action<TableOptions<TPrimaryKey, TValue>>? configureAction = null)
    {
        var tableOptions = new TableOptions<TPrimaryKey, TValue>();
        configureAction?.Invoke(tableOptions);
        return new RocksDbTable<TPrimaryKey, TValue>(rocksDb, primaryKeyProvider, primaryKeySerializer, valueSerializer, tableOptions);
    }

    internal static ChangeTransaction<RocksDbWrapper> CreateMockedTransaction(this RocksDb rocksDb, WriteOptions? writeOptions = null)
    {
        var wrapper = new RocksDbWrapper(rocksDb, writeOptions);
        return new ChangeTransaction<RocksDbWrapper>(null, wrapper, rocksDb, writeOptions);
    }
}