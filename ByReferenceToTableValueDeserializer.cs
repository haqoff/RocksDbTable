using System;
using Haqon.RocksDb.New;
using RocksDbSharp;

namespace Haqon.RocksDb;

public class ByReferenceToTableValueDeserializer<TPrimaryKey, TValue> : ISpanDeserializer<TValue>
{
    private readonly IRocksDbTable<TPrimaryKey, TValue> _table;

    public ByReferenceToTableValueDeserializer(IRocksDbTable<TPrimaryKey, TValue> table)
    {
        _table = table;
    }

    public TValue Deserialize(ReadOnlySpan<byte> buffer)
    {
        return _table.GetByKey(buffer);
    }
}