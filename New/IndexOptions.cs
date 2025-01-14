using System;
using RocksDbSharp;

namespace Haqon.RocksDb.New;

public class TableOptions
{
    internal string ColumnFamilyName { get; private set; } = Guid.NewGuid().ToString();
    internal ColumnFamilyOptions ColumnFamilyOptions { get; private set; } = new();

    public TableOptions SetColumnFamilyName(string name)
    {
        ColumnFamilyName = name;
        return this;
    }

    public TableOptions SetColumnFamilyOptions(ColumnFamilyOptions options)
    {
        ColumnFamilyOptions = options;
        return this;
    }
}

public class IndexOptions
{
    internal string ColumnFamilyName { get; private set; } = Guid.NewGuid().ToString();
    internal ColumnFamilyOptions ColumnFamilyOptions { get; private set; } = new();
    internal ValueStoreMode StoreMode { get; private set; } = ValueStoreMode.FullValue;

    public IndexOptions SetColumnFamilyName(string name)
    {
        ColumnFamilyName = name;
        return this;
    }

    public IndexOptions SetColumnFamilyOptions(ColumnFamilyOptions options)
    {
        ColumnFamilyOptions = options;
        return this;
    }

    public IndexOptions SetValueStoreMode(ValueStoreMode mode)
    {
        StoreMode = mode;
        return this;
    }
}