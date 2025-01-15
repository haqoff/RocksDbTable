using System;
using Haqon.RocksDb.ChangeTracking;
using RocksDbSharp;

namespace Haqon.RocksDb.Options;

/// <summary>
/// Represents configuration options for a RocksDB table.
/// </summary>
public class TableOptions<TKey, TValue> : IStoreOptions
{
    private string _columnFamilyName = Guid.NewGuid().ToString();
    private ColumnFamilyOptions _columnFamilyOptions = new();

    internal TableOptions()
    {
    }

    string IStoreOptions.ColumnFamilyName => _columnFamilyName;
    ColumnFamilyOptions IStoreOptions.ColumnFamilyOptions => _columnFamilyOptions;
    internal IRocksDbTableChangesConsumer<TKey, TValue>? ChangesConsumer { get; private set; }
    internal bool EnableConcurrentChangesWithinRow { get; private set; }
    internal int LockCount { get; private set; } = 64;

    /// <summary>
    /// Sets the name of the column family for this table.
    /// </summary>
    /// <remarks>
    /// By default, a random name is given.
    /// </remarks>
    /// <param name="name">The name of the column family.</param>
    /// <returns>The updated <see cref="TableOptions{TKey, TValue}"/> instance.</returns>
    public TableOptions<TKey, TValue> SetColumnFamilyName(string name)
    {
        _columnFamilyName = name;
        return this;
    }

    /// <summary>
    /// Sets the column family options for the table.
    /// </summary>
    /// <param name="options">The column family options to apply.</param>
    /// <returns>The current <see cref="IndexOptions"/> instance with the updated column family options.</returns>
    /// <remarks>
    /// By default, the column family options are set to a new instance of <see cref="ColumnFamilyOptions"/>.
    /// </remarks>
    /// <returns>The updated <see cref="TableOptions{TKey, TValue}"/> instance.</returns>
    public TableOptions<TKey, TValue> SetColumnFamilyOptions(ColumnFamilyOptions options)
    {
        _columnFamilyOptions = options;
        return this;
    }

    /// <summary>
    /// Configures a consumer for tracking changes in the table.
    /// </summary>
    /// <param name="consumer">The changes consumer to use.</param>
    /// <returns>The updated <see cref="TableOptions{TKey, TValue}"/> instance.</returns>
    public TableOptions<TKey, TValue> SetTableChangesConsumer(IRocksDbTableChangesConsumer<TKey, TValue> consumer)
    {
        ChangesConsumer = consumer;
        return this;
    }

    /// <summary>
    /// Enables or disables concurrent modifications within a single row.
    /// When enabled, row-level locking allows parallel operations within the same row.
    /// </summary>
    /// <remarks>
    /// Disabled by default.
    /// <br/>
    /// Indicates whether concurrent modifications within a single row are enabled.
    /// When enabled, row-level locking ensures thread-safe operations like <c>Put</c>, <c>Remove</c> and etc.
    /// Useful when indices are added to the table or when a changes consumer is configured.
    /// </remarks>
    /// <param name="enableRowConcurrency">True to enable row-level concurrency; otherwise, false.</param>
    /// <returns>The updated <see cref="TableOptions{TKey, TValue}"/> instance.</returns>
    public TableOptions<TKey, TValue> SetEnableConcurrentChangesWithinRow(bool enableRowConcurrency)
    {
        EnableConcurrentChangesWithinRow = enableRowConcurrency;
        return this;
    }

    /// <summary>
    /// Sets the number of locks for row-level concurrency control.
    /// </summary>
    /// <remarks>
    /// Default value is 64.
    /// Relevant only when <see cref="SetEnableConcurrentChangesWithinRow"/> is enabled.
    /// </remarks>
    /// <param name="lockCount">The number of locks. Must be greater than zero.</param>
    /// <returns>The updated <see cref="TableOptions{TKey, TValue}"/> instance.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="lockCount"/> is zero or negative.</exception>
    public TableOptions<TKey, TValue> SetLockCount(int lockCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(lockCount, nameof(lockCount));
        LockCount = lockCount;
        return this;
    }
}