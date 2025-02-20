namespace RocksDbTable.Tracing;

internal static class ActivityNames
{
    public const string NotUniqueIndexRemove = "RocksDb.NotUniqueIndex.Remove";
    public const string NotUniqueIndexPut = "RocksDb.NotUniqueIndex.Put";
    public const string NotUniqueIndexPutRemoveOldKey = "RocksDb.NotUniqueIndex.Put.RemoveOldKey";

    public const string UniqueIndexRemove = "RocksDb.UniqueIndex.Remove";
    public const string UniqueIndexPut = "RocksDb.UniqueIndex.Put";
    public const string UniqueIndexPutRemoveOldKey = "RocksDb.UniqueIndex.Put.RemoveOldKey";


    public const string TableTryApplyChangeNoTransactionSpecified = "RocksDb.Table.TryApplyChange";
    public const string TableTryApplyChangeCore = "RocksDb.Table.TryApplyChange.Core";
    public const string TableRemoveNoTransactionSpecified = "RocksDb.Table.Remove";
    public const string TableRemoveCore = "RocksDb.Table.Remove.Core";
    public const string TablePutNoTransactionSpecified = "RocksDb.Table.Put";
    public const string TablePutCore = "RocksDb.Table.Put.Core";

    public const string GetByKeySpan = "RocksDb.GetByKeySpan";
    public const string GetByKey = "RocksDb.GetByKey";
    public const string HasExactKey = "RocksDb.HasExactKey";
    public const string HasAnyKeyByPrefix = "RocksDb.HasAnyKeyByPrefix";
    public const string GetAllKeys = "RocksDb.GetAllKeys";
    public const string GetAllValues = "RocksDb.GetAllValues";
    public const string GetAllKeysByPrefix = "RocksDb.GetAllKeysByPrefix";
    public const string GetAllValuesByPrefix = "RocksDb.GetAllValuesByPrefix";
    public const string GetFirstValueByPrefix = "RocksDb.GetFirstValueByPrefix";
    public const string GetAllValuesByBounds = "RocksDb.GetAllValuesByBounds";

    public const string ChangeTransactionCommit = "RocksDb.ChangeTransaction.Commit";
    public const string ChangeTransactionCommitDispatchChanges = "RocksDb.ChangeTransaction.Commit.DispatchChanges";
}