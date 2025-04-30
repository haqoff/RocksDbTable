using RocksDbSharp;

namespace RocksDbTable.Options;

/// <summary>
/// Provides basic settings for creating a ColumnFamily in RocksDb.
/// </summary>
public interface IStoreOptions
{
    internal string ColumnFamilyName { get; }
    internal ColumnFamilyOptions ColumnFamilyOptions { get; }

    /// <summary>
    /// Sets the columnFamily name to use.
    /// </summary>
    void SetColumnFamilyName(string name);

    /// <summary>
    /// Sets ColumnFamily options.
    /// </summary>
    void SetColumnFamilyOptions(ColumnFamilyOptions options);
}