using System.Buffers;
using MemoryPack;
using MemoryPack.Formatters;

namespace Tests.Infrastructure;

public record struct MemoryPackWithDateOnlyModel(DateOnly Date, int IntValue, string StringValue) : IMemoryPackable<MemoryPackWithDateOnlyModel>
{
    public static void RegisterFormatter()
    {
        if (!MemoryPackFormatterProvider.IsRegistered<MemoryPackWithDateOnlyModel>())
        {
            MemoryPackFormatterProvider.Register(new MemoryPackableFormatter<MemoryPackWithDateOnlyModel>());
        }

        if (!MemoryPackFormatterProvider.IsRegistered<MemoryPackWithDateOnlyModel[]>())
        {
            MemoryPackFormatterProvider.Register(new ArrayFormatter<MemoryPackWithDateOnlyModel>());
        }
    }

    public static void Serialize<TBufferWriter>(ref MemoryPackWriter<TBufferWriter> writer, scoped ref MemoryPackWithDateOnlyModel value) where TBufferWriter : IBufferWriter<byte>
    {
        writer.WriteUnmanaged(value.Date, value.IntValue);
        writer.WriteString(value.StringValue);
    }

    public static void Deserialize(ref MemoryPackReader reader, scoped ref MemoryPackWithDateOnlyModel value)
    {
        reader.ReadUnmanaged(out DateOnly date, out int intValue);
        var stringValue = reader.ReadString()!;
        value = new MemoryPackWithDateOnlyModel(date, intValue, stringValue);
    }
}