using System;
using System.Buffers;

namespace RocksDbTable.Serialization;

/// <summary>
/// Defines methods for serializing and deserializing objects of type <typeparamref name="T"/> 
/// to and from a binary representation, typically used for storing data in RocksDb.
/// </summary>
/// <typeparam name="T">The type of the object to serialize and deserialize.</typeparam>
public interface IRockSerializer<T>
{
    /// <summary>
    /// Serializes the specified <paramref name="value"/> and writes the binary data 
    /// to the provided <paramref name="writer"/>.
    /// </summary>
    /// <param name="writer">The buffer writer to which the serialized data will be written.</param>
    /// <param name="value">The object to serialize.</param>
    void Serialize(IBufferWriter<byte> writer, T value);

    /// <summary>
    /// Deserializes an object of type <typeparamref name="T"/> from the given binary data.
    /// </summary>
    /// <param name="span">The read-only span containing the binary data to deserialize.</param>
    /// <returns>The deserialized object of type <typeparamref name="T"/>.</returns>
    T Deserialize(ReadOnlySpan<byte> span);
}