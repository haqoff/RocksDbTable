using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RocksDbSharp;

namespace Haqon.RocksDb;

public class RocksDbStatisticReceiver : BackgroundService, IRocksDbStatisticReceiver
{
    private readonly RocksDbStatisticConfig _config;
    private readonly ILogger<RocksDbStatisticReceiver> _logger;
    private readonly List<(string path, DbOptions opt, RocksDb db)> _list = new();
    private readonly object _lock = new();

    public RocksDbStatisticReceiver(IOptions<RocksDbConfig> options, ILogger<RocksDbStatisticReceiver> logger)
    {
        _config = options.Value.RocksDbStatistic;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Collect)
        {
            _logger.LogInformation("RocksDb metrics disabled.");
            return Task.CompletedTask;
        }

        return Task.Run(() => ExecuteCoreAsync(stoppingToken), stoppingToken);
    }

    private async Task ExecuteCoreAsync(CancellationToken cancellationToken)
    {
        // TODO: лучше отправлять в метрики, чем в лог
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            try
            {
                lock (_lock)
                {
                    foreach (var item in _list)
                    {
                        var statsSource = item.opt.GetStatisticsString();
                        var parsedDict = ParseStatistic(statsSource);
                        foreach (var property in _config.Properties)
                        {
                            var value = item.db.GetProperty(property);
                            parsedDict.Add(property, value);
                        }

                        using var scope = _logger.BeginScope(parsedDict);
                        _logger.LogInformation("RocksDb statistic collected for {path}", item.path);
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "Error when collecting rocksDb statistic.");
            }
        }
    }

    public void Add(string path, DbOptions options, RocksDb db)
    {
        lock (_lock)
        {
            _list.Add((path, options, db));
        }
    }

    public void Remove(RocksDb db)
    {
        lock (_lock)
        {
            var index = _list.FindIndex(i => ReferenceEquals(db, i.db));
            if (index >= 0)
            {
                _list.RemoveAt(index);
            }
        }
    }

    private Dictionary<string, object> ParseStatistic(string src)
    {
        var result = new Dictionary<string, object>();
        foreach (var line in src.AsSpan().EnumerateLines())
        {
            var colonIndex = line.IndexOf(':');
            if (colonIndex < 0)
            {
                continue;
            }

            var key = line.Slice(0, colonIndex).ToString();
            var value = line.Slice(colonIndex + 1).ToString();

            if (_config.Metrics.Count == 0 || _config.Metrics.Any(m => key.Contains(m)))
            {
                result.Add(key, value);
            }
        }

        return result;
    }
}