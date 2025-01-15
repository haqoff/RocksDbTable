using System.Buffers;
using System.Text;
using Haqon.RocksDb.Serialization;

namespace Tests;

public class StringRockSerializer : IRockSerializer<string>
{
    public static readonly StringRockSerializer Utf8 = new(Encoding.UTF8);

    private readonly Encoding _encoding;

    public StringRockSerializer(Encoding encoding)
    {
        _encoding = encoding;
    }

    public void Serialize<TWriter>(scoped ref TWriter writer, scoped in string value) where TWriter : IBufferWriter<byte>
    {
        var span = writer.GetSpan(value.Length);
        var written = _encoding.GetBytes(value, span);
        writer.Advance(written);
    }

    public string Deserialize(ReadOnlySpan<byte> span)
    {
        return _encoding.GetString(span);
    }
}