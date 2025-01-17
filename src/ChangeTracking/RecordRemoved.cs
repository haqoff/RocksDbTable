namespace RocksDbTable.ChangeTracking;

internal class RecordRemoved<TKey, TValue> : ITableChange
{
    public RecordRemoved(TKey newKey, TValue currentValue, IRocksDbTableChangesConsumer<TKey, TValue> consumer)
    {
        NewKey = newKey;
        CurrentValue = currentValue;
        Consumer = consumer;
    }

    public TKey NewKey { get; }
    public TValue CurrentValue { get; }
    public IRocksDbTableChangesConsumer<TKey, TValue> Consumer { get; }

    public void Dispatch()
    {
        Consumer.Removed(NewKey, CurrentValue);
    }
}