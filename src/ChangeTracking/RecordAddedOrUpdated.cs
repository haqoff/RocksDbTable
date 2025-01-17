namespace RocksDbTable.ChangeTracking;

internal class RecordAddedOrUpdated<TKey, TValue> : ITableChange
{
    public RecordAddedOrUpdated(TKey newKey, TValue? oldValue, TValue newValue, IRocksDbTableChangesConsumer<TKey, TValue> consumer)
    {
        NewKey = newKey;
        OldValue = oldValue;
        NewValue = newValue;
        Consumer = consumer;
    }

    public TKey NewKey { get; }
    public TValue? OldValue { get; }
    public TValue NewValue { get; }
    public IRocksDbTableChangesConsumer<TKey, TValue> Consumer { get; }

    public void Dispatch()
    {
        Consumer.AddedOrUpdated(NewKey, OldValue, NewValue);
    }
};