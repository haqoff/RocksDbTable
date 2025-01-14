namespace Haqon.RocksDb;

public interface IChangeConsumer<in TKey, in TValue>
{
    void Updated(TKey latestKey, TValue? oldValue, TValue? newValue);
}