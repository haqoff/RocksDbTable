using System.Collections.Generic;

namespace Haqon.RocksDb;

public class RocksDbStatisticConfig
{
    public bool Collect { get; init; } = false;
    public IReadOnlyList<string> Metrics { get; init; } = [];
    public IReadOnlyList<string> Properties { get; init; } = [];
}