using System;
using Haqon.RocksDb.Transactions;

namespace Haqon.RocksDb.Core;

internal interface IDependentIndex<in TValue>
{
    void Remove<TWrapper>(ReadOnlySpan<byte> primaryKeySpan, TValue value, ref ChangeTransaction<TWrapper> transaction)
        where TWrapper : IRocksDbCommandWrapper;

    void Put<TWrapper>(ReadOnlySpan<byte> primaryKeySpan, ReadOnlySpan<byte> valueSpan, TValue? oldValue, TValue newValue, ref ChangeTransaction<TWrapper> transaction)
        where TWrapper : IRocksDbCommandWrapper;
}