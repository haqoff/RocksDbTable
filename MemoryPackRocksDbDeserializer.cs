using System;
using MemoryPack;
using RocksDbSharp;

namespace Haqon.RocksDb;

public class MemoryPackRocksDbDeserializer<T> : ISpanDeserializer<T?>
{
    public static readonly MemoryPackRocksDbDeserializer<T> Default = new();

    public T? Deserialize(ReadOnlySpan<byte> buffer)
    {
        return MemoryPackSerializer.Deserialize<T>(buffer);
    }
}