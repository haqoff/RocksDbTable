using System.Buffers;
using RocksDbTable.Serialization;

namespace Tests.Infrastructure;

public class CounterDataSerializer : IRockSerializer<CounterData>
{
    public static readonly CounterDataSerializer Instance = new();
    
    public void Serialize(IBufferWriter<byte> writer, CounterData value)
    {
        Int32RockSerializer.Instance.Serialize(writer, value.Value);
        StringRockSerializer.Utf8.Serialize(writer, value.Name);
    }

    public CounterData Deserialize(ReadOnlySpan<byte> span)
    {
        var value = Int32RockSerializer.Instance.Deserialize(span.Slice(0, 4));
        var name = StringRockSerializer.Utf8.Deserialize(span.Slice(4));
        return new CounterData(name, value);
    }
}