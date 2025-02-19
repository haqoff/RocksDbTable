using Microsoft.Extensions.ObjectPool;

namespace RocksDbTable.Utils;

internal class ArrayPoolBufferWriterObjectPolicy : PooledObjectPolicy<ArrayPoolBufferWriter>
{
    public override ArrayPoolBufferWriter Create()
    {
        return new ArrayPoolBufferWriter(fromPool: true);
    }

    public override bool Return(ArrayPoolBufferWriter writer)
    {
        writer.Dispose();
        return true;
    }
}