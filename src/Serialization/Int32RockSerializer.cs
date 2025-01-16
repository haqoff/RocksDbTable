using System;
using System.Buffers;

namespace Haqon.RocksDb.Serialization;

public class Int32RockSerializer : IRockSerializer<int>
{
    public static readonly Int32RockSerializer Instance = new();

    internal Int32RockSerializer()
    {
    }

    public void Serialize(IBufferWriter<byte> writer, int value)
    {
        var span = writer.GetSpan(4);
        BitConverter.TryWriteBytes(span, value);
        writer.Advance(4);
    }

    public int Deserialize(ReadOnlySpan<byte> span)
    {
        return BitConverter.ToInt32(span);
    }
}