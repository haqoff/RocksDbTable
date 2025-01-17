using System.Collections.Generic;
using RocksDbTable.Core;
using RocksDbTable.Serialization;

namespace RocksDbTable.UniqueIndexes;

public interface IUniqueIndex<TUniqueKey, out TValue> : IKeyValueStoreBase<TUniqueKey, TValue>
{
    /// <summary>
    /// Retrieves a value associated with the specified unique key.
    /// </summary>
    /// <param name="uniqueKey">The unique key.</param>
    /// <returns>The value associated with the unique key, or <c>default</c> if the key does not exist.</returns>
    TValue? GetByKey(TUniqueKey uniqueKey);

    /// <summary>
    /// Determines whether the store contains an exact match for the specified key.
    /// </summary>
    /// <param name="uniqueKey">The unique key to check.</param>
    /// <returns><c>true</c> if the store contains the specified key; otherwise, <c>false</c>.</returns>
    bool HasExactKey(TUniqueKey uniqueKey);

    /// <summary>
    /// Retrieves all keys in the store.
    /// </summary>
    /// <returns>An enumerable collection of all keys in the store.</returns>
    IEnumerable<TUniqueKey> GetAllKeys();

    /// <summary>
    /// Retrieves all keys that match the specified prefix.
    /// </summary>
    /// <typeparam name="TPrefixKey">The type of the prefix key.</typeparam>
    /// <param name="prefixKey">The prefix key.</param>
    /// <param name="prefixSerializer">The serializer for the prefix key.</param>
    /// <returns>An enumerable collection of keys that match the specified prefix.</returns>
    IEnumerable<TUniqueKey> GetAllKeysByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer);
}