namespace Haqon.RocksDb.ChangeTracking;

internal interface ITableChange
{
    void Dispatch();
}