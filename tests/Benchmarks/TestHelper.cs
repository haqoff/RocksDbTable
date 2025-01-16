using RocksDbSharp;

namespace Benchmarks;

public static class TestHelper
{
    public static RocksDb CreateTempRocksDb(out string path)
    {
        path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(path);
        var options = new DbOptions().SetCreateIfMissing(true).SetInfoLogLevel(InfoLogLevel.Fatal)
            .SetWriteBufferSize(1024 * 1024 * 1024);
        return RocksDb.Open(options, path, new ColumnFamilies());
    }
}