using System;

namespace Haqon.RocksDb.Tables;

internal interface ITableValueProvider<out TValue>
{
    TValue? GetByKey(ReadOnlySpan<byte> buffer);
}