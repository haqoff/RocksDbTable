using System;
using RocksDbSharp;

namespace Haqon.RocksDb;

internal class RocksDeserializerAdapter<T> : ISpanDeserializer<T>
{
    private readonly IRockSerializer<T> _serializer;

    public RocksDeserializerAdapter(IRockSerializer<T> serializer)
    {
        _serializer = serializer;
    }

    public T Deserialize(ReadOnlySpan<byte> buffer)
    {
        return _serializer.Deserialize(buffer);
    }
}