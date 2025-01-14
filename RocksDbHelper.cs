using System;
using System.IO;
using RocksDbSharp;

namespace Haqon.RocksDb;

public static class RocksDbHelper
{
    public static RocksDb OpenTempRocksDb(RocksDbInstanceConfig config, IRocksDbStatisticReceiver statisticReceiver, Action<DbOptions>? configureAction = null)
    {
        var writeBufferSize = (ulong)config.MemTableMaxSizeInMegabytes * 1024 * 1024;
        var options = new DbOptions()
            .SetCreateIfMissing()
            .SetWriteBufferSize(writeBufferSize)
            .SetDbWriteBufferSize(writeBufferSize)
            .SetTargetFileSizeBase(writeBufferSize)
            .SetMaxWriteBufferNumber(config.MaxMemTables)
            .SetInfoLogLevel(config.LogLevel);

        if (config.EnableStatistic)
        {
            options.EnableStatistics();
        }

        configureAction?.Invoke(options);
        if (Directory.Exists(config.DirPath))
        {
            Directory.Delete(config.DirPath, true);
        }

        var db = RocksDb.Open(options, config.DirPath, new ColumnFamilies());

        if (config.EnableStatistic)
        {
            statisticReceiver.Add(config.DirPath, options, db);
        }

        return db;
    }

    public static void DeleteRocksDb(RocksDb? db, IRocksDbStatisticReceiver statisticReceiver)
    {
        if (db is null)
        {
            return;
        }

        statisticReceiver.Remove(db);
        db.Dispose();
    }

    public static ColumnFamily CreateColumnFamily(this RocksDb db, string name, RocksDbInstanceConfig config, Action<ColumnFamilyOptions>? configureAction = null)
    {
        var writeBufferSize = (ulong)config.MemTableMaxSizeInMegabytes * 1024 * 1024;
        var opt = new ColumnFamilyOptions()
            .SetMaxWriteBufferNumber(config.MaxMemTables)
            .SetWriteBufferSize(writeBufferSize);
        configureAction?.Invoke(opt);
        var handle = db.CreateColumnFamily(opt, name);
        return new(handle, opt);
    }
}