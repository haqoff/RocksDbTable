using System;
using System.Buffers;
using System.Text;

namespace RocksDbTable.Serialization;

/// <summary>
/// A serializer for strings that uses a specified <see cref="Encoding"/> to convert
/// strings to and from a binary format suitable for storage in RocksDb.
/// </summary>
public class StringRockSerializer(Encoding encoding) : IRockSerializer<string>
{
    /// <summary>
    /// A predefined instance of <see cref="StringRockSerializer"/> that uses UTF-8 encoding.
    /// </summary>
    public static readonly StringRockSerializer Utf8 = new(Encoding.UTF8);

    /// <inheritdoc />
    public void Serialize(IBufferWriter<byte> writer, string value)
    {
        var span = writer.GetSpan(value.Length);
        var written = encoding.GetBytes(value, span);
        writer.Advance(written);
    }

    /// <inheritdoc />
    public string Deserialize(ReadOnlySpan<byte> span)
    {
        return encoding.GetString(span);
    }
}