using System.Collections.Generic;

namespace Haqon.RocksDb.New;

public interface INotUniqueIndex<TNotUniqueKey, out TValue>
{
    bool HasAny(in TNotUniqueKey key);
    TValue? GetFirstValue(in TNotUniqueKey key, SeekMode mode = SeekMode.SeekToFirst);
    TValue? GetFirstValueByPrefix<TPrefixKey>(in TPrefixKey prefixKey, IRockSerializer<TPrefixKey> serializer, SeekMode mode = SeekMode.SeekToFirst);
    IEnumerable<TValue> GetAllValuesByKey(TNotUniqueKey key);
    IEnumerable<TValue> GetAllValuesByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer);
    IEnumerable<TValue> GetAllValuesByBounds(TNotUniqueKey start, TNotUniqueKey end);
}