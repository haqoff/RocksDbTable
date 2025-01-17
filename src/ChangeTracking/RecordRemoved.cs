namespace RocksDbTable.ChangeTracking;

internal record RecordRemoved<TKey, TValue>(TKey NewKey, TValue CurrentValue, IRocksDbTableChangesConsumer<TKey, TValue> Consumer) : ITableChange
{
    public void Dispatch()
    {
        Consumer.Removed(NewKey, CurrentValue);
    }
}