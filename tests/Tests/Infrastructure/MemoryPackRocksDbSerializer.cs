using System.Buffers;
using MemoryPack;
using RocksDbTable.Serialization;

namespace Tests.Infrastructure;

public class MemoryPackRocksDbSerializer<T> : IRockSerializer<T>
{
    public static readonly MemoryPackRocksDbSerializer<T> Default = new();

    private MemoryPackRocksDbSerializer()
    {
    }

    public void Serialize(IBufferWriter<byte> writer, T? value)
    {
        MemoryPackSerializer.Serialize(writer, value);
    }

    public T Deserialize(ReadOnlySpan<byte> buffer)
    {
        return MemoryPackSerializer.Deserialize<T>(buffer)!;
    }
}