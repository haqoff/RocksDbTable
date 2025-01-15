namespace Haqon.RocksDb.ChangeTracking;

internal record RecordAddedOrUpdated<TKey, TValue>(TKey NewKey, TValue? OldValue, TValue NewValue, IRocksDbTableChangesConsumer<TKey, TValue> Consumer) : ITableChange
{
    public void Dispatch()
    {
        Consumer.AddedOrUpdated(NewKey, OldValue, NewValue);
    }
};