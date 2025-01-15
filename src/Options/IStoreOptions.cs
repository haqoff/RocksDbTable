using RocksDbSharp;

namespace Haqon.RocksDb.Options;

internal interface IStoreOptions
{
    internal string ColumnFamilyName { get; }
    internal ColumnFamilyOptions ColumnFamilyOptions { get; }
}