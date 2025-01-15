using RocksDbSharp;

namespace Tests;

public static class TestHelper
{
    public static RocksDb CreateTempRocksDb()
    {
        var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        var options = new DbOptions().SetCreateIfMissing(true);
        return RocksDb.Open(options, path);
    }
}