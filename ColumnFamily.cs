using RocksDbSharp;

namespace Haqon.RocksDb;

public readonly record struct ColumnFamily(ColumnFamilyHandle Handle, ColumnFamilyOptions Options);