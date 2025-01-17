namespace RocksDbTable.ChangeTracking;

internal interface ITableChange
{
    void Dispatch();
}