using System;
using RocksDbSharp;

namespace Haqon.RocksDb.Options;

/// <summary>
/// Specifies options for configuring an index.
/// </summary>
public class IndexOptions : IStoreOptions
{
    private string _columnFamilyName = Guid.NewGuid().ToString();
    private ColumnFamilyOptions _columnFamilyOptions = new();

    internal IndexOptions()
    {
    }

    string IStoreOptions.ColumnFamilyName => _columnFamilyName;
    ColumnFamilyOptions IStoreOptions.ColumnFamilyOptions => _columnFamilyOptions;
    internal ValueStoreMode StoreMode { get; private set; } = ValueStoreMode.FullValue;

    /// <summary>
    /// Sets the column family name for the index.
    /// </summary>
    /// <param name="name">The name of the column family.</param>
    /// <returns>The current <see cref="IndexOptions"/> instance with the updated column family name.</returns>
    /// <remarks>
    /// By default, the column family name is set to a newly generated GUID.
    /// </remarks>
    public IndexOptions SetColumnFamilyName(string name)
    {
        _columnFamilyName = name;
        return this;
    }

    /// <summary>
    /// Sets the column family options for the index.
    /// </summary>
    /// <param name="options">The column family options to apply.</param>
    /// <returns>The current <see cref="IndexOptions"/> instance with the updated column family options.</returns>
    /// <remarks>
    /// By default, the column family options are set to a new instance of <see cref="ColumnFamilyOptions"/>.
    /// </remarks>
    public IndexOptions SetColumnFamilyOptions(ColumnFamilyOptions options)
    {
        _columnFamilyOptions = options;
        return this;
    }

    /// <summary>
    /// Sets the value store mode for the index, specifying how the index value will be stored.
    /// </summary>
    /// <param name="mode">The value store mode to set. This determines whether the full value or the primary key is stored in the index.</param>
    /// <returns>
    /// The current <see cref="IndexOptions"/> instance with the updated store mode.
    /// </returns>
    /// <remarks>
    /// By default, the value store mode is set to <see cref="ValueStoreMode.FullValue"/>, where the entire value (e.g., a full row) is stored in the index. 
    /// You can set the store mode to <see cref="ValueStoreMode.Reference"/> if you want to store only the primary key in the index, which is more memory-efficient when working with large rows or tables.
    /// </remarks>
    public IndexOptions SetValueStoreMode(ValueStoreMode mode)
    {
        StoreMode = mode;
        return this;
    }
}