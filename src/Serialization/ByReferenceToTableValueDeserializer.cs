﻿using System;
using Haqon.RocksDb.Tables;
using RocksDbSharp;

namespace Haqon.RocksDb.Serialization;

internal class ByReferenceToTableValueDeserializer<TValue> : ISpanDeserializer<TValue>
{
    private readonly ITableValueProvider<TValue> _table;

    public ByReferenceToTableValueDeserializer(ITableValueProvider<TValue> table)
    {
        _table = table;
    }

    public TValue Deserialize(ReadOnlySpan<byte> buffer)
    {
        return _table.GetByKey(buffer)!;
    }
}