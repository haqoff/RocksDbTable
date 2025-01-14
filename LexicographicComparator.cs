using System;
using RocksDbSharp;

namespace Haqon.RocksDb;

public class LexicographicComparator : Comparator
{
    public static readonly LexicographicComparator Instance = new();

    private LexicographicComparator()
    {
    }

    public unsafe int Compare(IntPtr a, UIntPtr alen, IntPtr b, UIntPtr blen)
    {
        var aSpan = new ReadOnlySpan<byte>((byte*)a, (int)alen);
        var bSpan = new ReadOnlySpan<byte>((byte*)b, (int)blen);
        return Compare(aSpan, bSpan);
    }

    public static int Compare(ReadOnlySpan<byte> span1, ReadOnlySpan<byte> span2)
    {
        int minLength = Math.Min(span1.Length, span2.Length);

        for (int i = 0; i < minLength; i++)
        {
            if (span1[i] < span2[i])
            {
                return -1;
            }

            if (span1[i] > span2[i])
            {
                return 1;
            }
        }

        // Если все сравнимые байты равны, сравниваем длины
        if (span1.Length < span2.Length)
        {
            return -1;
        }

        if (span1.Length > span2.Length)
        {
            return 1;
        }

        return 0;
    }

    public string Name => "Lexicographic";
}