using RocksDbSharp;

namespace Haqon.RocksDb.Options;

internal static class SharedReadOptions
{
    public static readonly ReadOptions OnlyPrefixRead = new ReadOptions().SetPrefixSameAsStart(true);
}