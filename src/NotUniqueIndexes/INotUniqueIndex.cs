using System.Collections.Generic;
using Haqon.RocksDb.Core;
using Haqon.RocksDb.Options;

namespace Haqon.RocksDb.NotUniqueIndexes;

public interface INotUniqueIndex<in TNotUniqueKey, out TValue> : IKeyValueStoreBase<TNotUniqueKey, TValue>
{
    /// <summary>
    /// Determines whether any values are associated with the specified key.
    /// </summary>
    /// <param name="key">The key to check for associated values.</param>
    /// <returns><c>true</c> if there are any associated values; otherwise, <c>false</c>.</returns>
    bool HasAny(TNotUniqueKey key);

    /// <summary>
    /// Retrieves the first value associated with the specified key.
    /// </summary>
    /// <param name="key">The key to retrieve the first value for.</param>
    /// <param name="mode">The seek mode to use (e.g., SeekToFirst or SeekToPrev).</param>
    /// <returns>The first value associated with the key, or <c>null</c> if no values exist.</returns>
    TValue? GetFirstValue(TNotUniqueKey key, SeekMode mode = SeekMode.SeekToFirst);

    /// <summary>
    /// Retrieves all values associated with the specified key.
    /// </summary>
    /// <param name="key">The key to retrieve all associated values for.</param>
    /// <returns>An enumerable collection of values associated with the key.</returns>
    IEnumerable<TValue> GetAllValuesByKey(TNotUniqueKey key);
}