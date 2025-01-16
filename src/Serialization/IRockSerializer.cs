using System;
using System.Buffers;

namespace Haqon.RocksDb.Serialization;

public interface IRockSerializer<T>
{
    void Serialize(IBufferWriter<byte> writer, T value);
    T Deserialize(ReadOnlySpan<byte> span);
}