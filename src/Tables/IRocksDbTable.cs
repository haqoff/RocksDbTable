﻿using System;
using System.Collections.Generic;
using RocksDbSharp;
using RocksDbTable.NotUniqueIndexes;
using RocksDbTable.Options;
using RocksDbTable.Serialization;
using RocksDbTable.Transactions;
using RocksDbTable.UniqueIndexes;

namespace RocksDbTable.Tables;

/// <summary>
/// Table in the RocksDb database
/// </summary>
/// <typeparam name="TPrimaryKey">Type of primary key.</typeparam>
/// <typeparam name="TValue">Type of entry.</typeparam>
public interface IRocksDbTable<TPrimaryKey, TValue> : IUniqueIndex<TPrimaryKey, TValue>
{
    /// <summary>
    /// Performs an atomic update based on a row lock. (This is not a Merge operation from RocksDb)
    /// Makes sense to use if <see cref="TableOptions{TKey,TValue}.SetEnableConcurrentChangesWithinRow"/><c>(true)</c> was called.
    /// </summary>
    /// <param name="primaryKey">The primary key of the row to update.</param>
    /// <param name="change">The change to apply.</param>
    /// <param name="tryApplyDelegate">The delegate performing the update.</param>
    /// <param name="newValue">The new applied value, set via the <paramref name="tryApplyDelegate"/> delegate.</param>
    /// <param name="writeOptions">Optional write options for the operation.</param>
    /// <returns>Returns the same result as returned from the <paramref name="tryApplyDelegate"/> delegate. If <c>true</c>, then the value was applied to the store.</returns>
    bool TryApplyChange<TChange>(TPrimaryKey primaryKey, TChange change, ChangeApplierDelegate<TPrimaryKey, TValue, TChange> tryApplyDelegate, out TValue? newValue, WriteOptions? writeOptions = null);

    /// <summary>
    /// Performs an atomic update based on a row lock within the specified transaction. (This is not a Merge operation from RocksDb)
    /// Makes sense to use if <see cref="TableOptions{TKey,TValue}.SetEnableConcurrentChangesWithinRow"/><c>(true)</c> was called.
    /// </summary>
    /// <param name="primaryKey">The primary key of the row to update.</param>
    /// <param name="change">The change to apply.</param>
    /// <param name="tryApplyDelegate">The delegate performing the update.</param>
    /// <param name="newValue">The new applied value, set via the <paramref name="tryApplyDelegate"/> delegate.</param>
    /// <param name="transaction">The transaction within which the operation is performed.</param>
    /// <returns>Returns the same result as returned from the <paramref name="tryApplyDelegate"/> delegate. If <c>true</c>, then the value was applied to the store.</returns>
    bool TryApplyChange<TChange, TWrapper>(TPrimaryKey primaryKey, TChange change, ChangeApplierDelegate<TPrimaryKey, TValue, TChange> tryApplyDelegate, out TValue? newValue, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper;

    /// <summary>
    /// Removes an entry from the storage using the specified primary key.
    /// </summary>
    /// <param name="primaryKey">The primary key of the entry to remove.</param>
    /// <param name="writeOptions">Optional write options for the removal operation.</param>
    void Remove(TPrimaryKey primaryKey, WriteOptions? writeOptions = null);

    /// <summary>
    /// Removes an entry from the storage using the specified primary key and an existing transaction.
    /// </summary>
    /// <param name="primaryKey">The primary key of the entry to remove.</param>
    /// <param name="transaction">The transaction to use for the removal operation.</param>
    void Remove<TWrapper>(TPrimaryKey primaryKey, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper;

    /// <summary>
    /// Adds or updates an entry in the storage.
    /// </summary>
    /// <param name="newValue">The value to add or update in the storage.</param>
    /// <param name="writeOptions">Optional write options for the put operation.</param>
    void Put(TValue newValue, WriteOptions? writeOptions = null);

    /// <summary>
    /// Adds or updates an entry in the storage using an existing transaction.
    /// </summary>
    /// <typeparam name="TWrapper">The type of the command wrapper used for the transaction.</typeparam>
    /// <param name="value">The value to add or update in the storage.</param>
    /// <param name="transaction">The transaction to use for the put operation.</param>
    void Put<TWrapper>(TValue value, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper;

    /// <summary>
    /// Retrieves all values (entries) in the store.
    /// </summary>
    /// <returns>An enumerable collection of all keys in the store.</returns>
    IEnumerable<TValue> GetAllValues();

    /// <summary>
    /// Creates a unique index for the table.
    /// </summary>
    /// <typeparam name="TUniqueKey">The type of the unique key.</typeparam>
    /// <param name="uniqueKeyProvider">A function to provide the unique key from a value (from entry).</param>
    /// <param name="keySerializer">The serializer to use for the unique key.</param>
    /// <param name="configureAction">An optional action to configure index options.</param>
    /// <returns>A unique index for the table.</returns>
    IUniqueIndex<TUniqueKey, TValue> CreateUniqueIndex<TUniqueKey>(Func<TValue, TUniqueKey> uniqueKeyProvider, IRockSerializer<TUniqueKey> keySerializer, Action<IndexOptions>? configureAction = null);

    /// <summary>
    /// Creates a non-unique index for the table.
    /// </summary>
    /// <typeparam name="TNotUniqueKey">The type of the non-unique key.</typeparam>
    /// <param name="notUniqueKeyProvider">A function to provide the non-unique key from a value (from entry).</param>
    /// <param name="keySerializer">The serializer to use for the non-unique key.</param>
    /// <param name="configureAction">An optional action to configure index options.</param>
    /// <returns>A non-unique index for the table.</returns>
    INotUniqueIndex<TNotUniqueKey, TValue> CreateNotUniqueIndex<TNotUniqueKey>(Func<TValue, TNotUniqueKey> notUniqueKeyProvider, IRockSerializer<TNotUniqueKey> keySerializer, Action<IndexOptions>? configureAction = null);
}