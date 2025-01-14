using System;
using RocksDbSharp;

namespace Haqon.RocksDb;

public class Int32RocksDbDeserializer : ISpanDeserializer<int>
{
    public static readonly Int32RocksDbDeserializer Default = new();
    
    public int Deserialize(ReadOnlySpan<byte> buffer)
    {
        return BitConverter.ToInt32(buffer);
    }
}