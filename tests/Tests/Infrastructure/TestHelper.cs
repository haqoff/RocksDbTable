using RocksDbSharp;

namespace Tests.Infrastructure;

public static class TestHelper
{
    public static RocksDb CreateTempRocksDb(out string path)
    {
        path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(path);
        var options = new DbOptions().SetCreateIfMissing(true);
        return RocksDb.Open(options, path, new ColumnFamilies());
    }
}