namespace RocksDbTable.ChangeTracking;

/// <summary>
/// A consumer subscribed to changes in a RocksDb storage table.
/// </summary>
/// <typeparam name="TKey">The type of the key.</typeparam>
/// <typeparam name="TValue">The type of the value (record).</typeparam>
public interface IRocksDbTableChangesConsumer<in TKey, in TValue>
{
    /// <summary>
    /// Occurs when a value (record) is removed from the storage.
    /// </summary>
    /// <param name="key">The primary key.</param>
    /// <param name="removedValue">The removed value.</param>
    void Removed(TKey key, TValue removedValue);

    /// <summary>
    /// Occurs when a record is added or updated.
    /// </summary>
    /// <param name="key">The primary key.</param>
    /// <param name="oldValue">The old value that was in the storage, or <c>default</c> if the record was added.</param>
    /// <param name="newValue">The new, current value.</param>
    void AddedOrUpdated(TKey key, TValue? oldValue, TValue newValue);
}