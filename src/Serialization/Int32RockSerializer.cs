using System;
using System.Buffers;

namespace RocksDbTable.Serialization;

/// <summary>
/// Number serializer <see cref="int"/>.
/// </summary>
public class Int32RockSerializer : IRockSerializer<int>
{
    /// <summary>
    /// Instance.
    /// </summary>
    public static readonly Int32RockSerializer Instance = new();

    private Int32RockSerializer()
    {
    }

    /// <inheritdoc />
    public void Serialize(IBufferWriter<byte> writer, int value)
    {
        var span = writer.GetSpan(4);
        BitConverter.TryWriteBytes(span, value);
        writer.Advance(4);
    }

    /// <inheritdoc />
    public int Deserialize(ReadOnlySpan<byte> span)
    {
        return BitConverter.ToInt32(span);
    }
}