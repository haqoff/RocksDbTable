using System.Collections.Generic;

namespace Haqon.RocksDb.New;

public interface IUniqueIndex<TUniqueKey, out TValue>
{
    TValue? GetByKey(in TUniqueKey uniqueKey);
    bool HasKey(in TUniqueKey uniqueKey);
    IEnumerable<TUniqueKey> GetAllKeys();
    TValue? GetFirstValueByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> serializer, SeekMode mode = SeekMode.SeekToFirst);
    IEnumerable<TUniqueKey> GetAllKeysByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer);
    IEnumerable<TValue> GetAllValuesByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer);
}