using System;
using RocksDbTable.Options;
using RocksDbTable.Transactions;

namespace RocksDbTable.Core;

internal interface IDependentIndex<in TValue>
{
    internal IStoreOptions StoreOptions { get; }

    void Remove<TWrapper>(ReadOnlySpan<byte> primaryKeySpan, TValue value, ref ChangeTransaction<TWrapper> transaction)
        where TWrapper : IRocksDbCommandWrapper;

    void Put<TWrapper>(ReadOnlySpan<byte> primaryKeySpan, ReadOnlySpan<byte> valueSpan, TValue? oldValue, TValue newValue, ref ChangeTransaction<TWrapper> transaction)
        where TWrapper : IRocksDbCommandWrapper;
}