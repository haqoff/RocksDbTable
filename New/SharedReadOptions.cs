using RocksDbSharp;

namespace Haqon.RocksDb.New;

internal static class SharedReadOptions
{
    public static readonly ReadOptions OnlyPrefixRead = new ReadOptions().SetPrefixSameAsStart(true);
}