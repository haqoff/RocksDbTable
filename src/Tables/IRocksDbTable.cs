using System;
using System.Collections.Generic;
using Haqon.RocksDb.NotUniqueIndexes;
using Haqon.RocksDb.Options;
using Haqon.RocksDb.Serialization;
using Haqon.RocksDb.Transactions;
using Haqon.RocksDb.UniqueIndexes;
using RocksDbSharp;

namespace Haqon.RocksDb.Tables;

public interface IRocksDbTable<TPrimaryKey, TValue> : IUniqueIndex<TPrimaryKey, TValue>
{
    void Remove(in TPrimaryKey primaryKey, WriteOptions? writeOptions = null);
    void Remove<TWrapper>(in TPrimaryKey primaryKey, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper;

    void Put(in TValue newValue, WriteOptions? writeOptions = null);
    void Put<TWrapper>(in TValue value, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper;
    
    IEnumerable<TValue> GetAllValues();

    IUniqueIndex<TUniqueKey, TValue> CreateUniqueIndex<TUniqueKey>(Func<TValue, TUniqueKey> uniqueKeyProvider, IRockSerializer<TUniqueKey> keySerializer, Action<IndexOptions>? configureAction = null);
    INotUniqueIndex<TNotUniqueKey, TValue> CreateNotUniqueIndex<TNotUniqueKey>(Func<TValue, TNotUniqueKey> notUniqueKeyProvider, IRockSerializer<TNotUniqueKey> keySerializer, Action<IndexOptions>? configureAction = null);
}