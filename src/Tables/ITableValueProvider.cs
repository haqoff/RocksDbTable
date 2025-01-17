using System;

namespace RocksDbTable.Tables;

internal interface ITableValueProvider<out TValue>
{
    TValue? GetByKey(ReadOnlySpan<byte> buffer);
}