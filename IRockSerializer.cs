using System;
using System.Buffers;

namespace Haqon.RocksDb;

public interface IRockSerializer<T>
{
    void Serialize<TWriter>(scoped ref TWriter writer, scoped in T value) where TWriter : IBufferWriter<byte>;
    T Deserialize(ReadOnlySpan<byte> span);
}