using System;
using System.Buffers;
using System.Text;

namespace Haqon.RocksDb.Serialization;

public class StringRockSerializer(Encoding encoding) : IRockSerializer<string>
{
    public static readonly StringRockSerializer Utf8 = new(Encoding.UTF8);

    public void Serialize(IBufferWriter<byte> writer, string value)
    {
        var span = writer.GetSpan(value.Length);
        var written = encoding.GetBytes(value, span);
        writer.Advance(written);
    }

    public string Deserialize(ReadOnlySpan<byte> span)
    {
        return encoding.GetString(span);
    }
}