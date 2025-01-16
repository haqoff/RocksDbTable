using System;
using Haqon.RocksDb.Utils;

namespace Haqon.RocksDb.Core;

internal readonly ref struct SpanPoint
{
    private readonly int _start;
    private readonly int _length;

    public SpanPoint(ArrayPoolBufferWriter writer, SpanPoint prev = default)
    {
        _start = prev._start + prev._length;
        _length = writer.WrittenSpan.Length - _start;
    }

    public ReadOnlySpan<byte> GetWrittenSpan(ArrayPoolBufferWriter writer)
    {
        return writer.WrittenSpan.Slice(_start, _length);
    }
}