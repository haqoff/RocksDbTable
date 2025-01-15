namespace Haqon.RocksDb.Options;

/// <summary>
/// Specifies how the index value will be stored.
/// </summary>
public enum ValueStoreMode : byte
{
    /// <summary>
    /// The full value (e.g., the entire "row") is stored in the index, meaning the value is duplicated.
    /// </summary>
    /// <remarks>
    /// This mode is preferred in most cases for performance.
    /// </remarks>
    FullValue = 0,

    /// <summary>
    /// The primary key of the table is stored in the index as the value.
    /// When retrieving by the index key, the primary key is first fetched, and then the corresponding row is retrieved from the table using the primary key.
    /// This approach is useful when the table contains a large amount of data or if the rows are "heavy" (i.e., large).
    /// It is particularly advantageous if the size of the primary key is much smaller than the size of the full row.
    /// </summary>
    /// <remarks>
    /// This mode is preferred if the primary key is smaller than the value (row) and storage memory needs to be saved.
    /// </remarks>
    Reference = 1
}