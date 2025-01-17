using System;
using System.Buffers;

namespace RocksDbTable.Serialization;

public interface IRockSerializer<T>
{
    void Serialize(IBufferWriter<byte> writer, T value);
    T Deserialize(ReadOnlySpan<byte> span);
}