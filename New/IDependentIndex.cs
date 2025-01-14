using System;

namespace Haqon.RocksDb.New;

internal interface IDependentIndex<in TValue>
{
    void Remove<TWrapper>(in ReadOnlySpan<byte> primaryKeySpan, TValue value, ref ChangeTransaction<TWrapper> transaction)
        where TWrapper : IRocksDbCommandWrapper;

    void Put<TWrapper>(in ReadOnlySpan<byte> primaryKeySpan, in ReadOnlySpan<byte> valueSpan, TValue? oldValue, TValue newValue, ref ChangeTransaction<TWrapper> transaction)
        where TWrapper : IRocksDbCommandWrapper;
}