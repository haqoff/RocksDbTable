using RocksDbSharp;

namespace RocksDbTable.Options;

internal interface IStoreOptions
{
    internal string ColumnFamilyName { get; }
    internal ColumnFamilyOptions ColumnFamilyOptions { get; }
}