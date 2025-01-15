using System;
using RocksDbSharp;

namespace Haqon.RocksDb.Serialization;

internal class RocksDbSpanDeserializerAdapter<T> : ISpanDeserializer<T>
{
    private readonly IRockSerializer<T> _serializer;

    public RocksDbSpanDeserializerAdapter(IRockSerializer<T> serializer)
    {
        _serializer = serializer;
    }

    public T Deserialize(ReadOnlySpan<byte> buffer)
    {
        return _serializer.Deserialize(buffer);
    }
}