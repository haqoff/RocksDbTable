using System;
using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace RocksDbTable.Utils;

internal sealed class ArrayPoolBufferWriter : IBufferWriter<byte>, IDisposable
{
    private readonly int _initialCapacity;
    private byte[]? _rentedBuffer;
    private int _writtenSize;

    public ArrayPoolBufferWriter(int initialCapacity = 256)
    {
        Debug.Assert(initialCapacity > 0);
        _initialCapacity = initialCapacity;
        _writtenSize = 0;
    }

    public int WrittenCount => _writtenSize;
    public int Capacity => _rentedBuffer?.Length ?? 0;
    public int FreeCapacity => Capacity - _writtenSize;
    public ReadOnlyMemory<byte> WrittenMemory => _rentedBuffer == null ? ReadOnlyMemory<byte>.Empty : _rentedBuffer.AsMemory(0, _writtenSize);
    public ReadOnlySpan<byte> WrittenSpan => _rentedBuffer == null ? ReadOnlySpan<byte>.Empty : _rentedBuffer.AsSpan(0, _writtenSize);

    public void Reset()
    {
        _writtenSize = 0;
    }

    public void Advance(int count)
    {
        Debug.Assert(_rentedBuffer != null);
        Debug.Assert(count >= 0);
        Debug.Assert(_writtenSize <= _rentedBuffer.Length - count);
        _writtenSize += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        CheckAndResizeBuffer(sizeHint);
        return _rentedBuffer.AsMemory(_writtenSize);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        CheckAndResizeBuffer(sizeHint);
        return _rentedBuffer.AsSpan(_writtenSize);
    }

    public byte[] GetUnderlyingArray()
    {
        return _rentedBuffer ?? [];
    }

    public void Dispose()
    {
        _writtenSize = 0;
        if (_rentedBuffer != null)
        {
            ArrayPool<byte>.Shared.Return(_rentedBuffer);
            _rentedBuffer = null;
        }
    }

    private void CheckAndResizeBuffer(int sizeHint)
    {
        Debug.Assert(sizeHint >= 0);
        if (_rentedBuffer is null)
        {
            _rentedBuffer = ArrayPool<byte>.Shared.Rent(_initialCapacity >= sizeHint ? _initialCapacity : sizeHint);
            Array.Clear(_rentedBuffer, 0, _rentedBuffer.Length);
        }
        else
        {
            int availableSpace = _rentedBuffer.Length - _writtenSize;
            if (sizeHint > availableSpace)
            {
                int currentLength = _rentedBuffer.Length;
                int growBy = Math.Max(sizeHint, currentLength);
                int newSize = currentLength + growBy;

                if ((uint)newSize > int.MaxValue)
                {
                    newSize = currentLength + sizeHint;
                    if ((uint)newSize > int.MaxValue)
                    {
                        ThrowHelper.BufferMaximumSizeExceeded();
                    }
                }

                byte[] oldBuffer = _rentedBuffer;
                _rentedBuffer = ArrayPool<byte>.Shared.Rent(newSize);

                if (_rentedBuffer.Length > _writtenSize)
                {
                    Array.Clear(_rentedBuffer, _writtenSize, _rentedBuffer.Length - _writtenSize);
                }

                Debug.Assert(oldBuffer.Length >= _writtenSize);
                Debug.Assert(_rentedBuffer.Length >= _writtenSize);

                oldBuffer.AsSpan(0, _writtenSize).CopyTo(_rentedBuffer);
                ArrayPool<byte>.Shared.Return(oldBuffer);
            }
        }

        Debug.Assert(_rentedBuffer.Length - _writtenSize >= sizeHint);
    }

    internal static class ThrowHelper
    {
        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void BufferMaximumSizeExceeded()
        {
            throw new OutOfMemoryException();
        }
    }
}