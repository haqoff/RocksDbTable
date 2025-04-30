namespace RocksDbTable.Options;

/// <summary>
/// The seek mode to use.
/// </summary>
public enum SeekMode : byte
{
    /// <summary>
    ///  Будет искать первый ключ, который равен целевому ключу.
    /// </summary>
    SeekToFirst = 0,

    /// <summary>
    /// Будет искать последний ключ, который меньше или равен целевому ключу.
    /// </summary>
    SeekToPrev = 1
}