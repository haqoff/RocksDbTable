namespace Haqon.RocksDb;

public interface IChange
{
    void Dispatch();
}

public readonly record struct Change<TKey, TValue> : IChange
{
    public Change(TKey NewKey, TValue? OldValue, TValue? NewValue, IChangeConsumer<TKey, TValue> Consumer)
    {
        this.NewKey = NewKey;
        this.OldValue = OldValue;
        this.NewValue = NewValue;
        this.Consumer = Consumer;
    }

    public void Dispatch()
    {
        Consumer.Updated(NewKey, OldValue, NewValue);
    }

    public TKey NewKey { get; }
    public TValue? OldValue { get; }
    public TValue? NewValue { get; }
    public IChangeConsumer<TKey, TValue> Consumer { get; }


};