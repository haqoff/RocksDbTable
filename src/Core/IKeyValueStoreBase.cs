using System.Collections.Generic;
using RocksDbTable.Options;
using RocksDbTable.Serialization;

namespace RocksDbTable.Core;

public interface IKeyValueStoreBase<in TKey, out TValue>
{
    /// <summary>
    /// Retrieves the first value that matches the specified key prefix.
    /// </summary>
    /// <typeparam name="TPrefixKey">The type of the prefix key.</typeparam>
    /// <param name="prefixKey">The prefix key.</param>
    /// <param name="serializer">The serializer for the prefix key.</param>
    /// <param name="mode">The seek mode to use when searching for the value.</param>
    /// <returns>The first value that matches the prefix, or <c>null</c> if no value matches.</returns>
    TValue? GetFirstValueByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> serializer, SeekMode mode = SeekMode.SeekToFirst);

    /// <summary>
    /// Retrieves all values that match the specified key prefix.
    /// </summary>
    /// <typeparam name="TPrefixKey">The type of the prefix key.</typeparam>
    /// <param name="prefixKey">The prefix key.</param>
    /// <param name="prefixSerializer">The serializer for the prefix key.</param>
    /// <returns>An enumerable collection of values that match the specified key prefix.</returns>
    IEnumerable<TValue> GetAllValuesByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer);

    /// <summary>
    /// Retrieves all values that fall within the specified range of keys.
    /// </summary>
    /// <param name="startInclusive">The starting key of the range (inclusive).</param>
    /// <param name="endExclusive">The ending key of the range (exclusive).</param>
    /// <returns>An enumerable collection of values within the specified range.</returns>
    IEnumerable<TValue> GetAllValuesByBounds(TKey startInclusive, TKey endExclusive);

    /// <summary>
    /// Checks if the store contains any key that matches the specified prefix.
    /// </summary>
    /// <typeparam name="TPrefixKey">The type of the prefix key.</typeparam>
    /// <param name="key">The prefix key.</param>
    /// <param name="serializer">The serializer for the prefix key.</param>
    /// <returns><c>true</c> if at least one key matches the prefix; otherwise, <c>false</c>.</returns>
    bool HasAnyKeyByPrefix<TPrefixKey>(TPrefixKey key, IRockSerializer<TPrefixKey> serializer);
}