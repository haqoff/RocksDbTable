using System.Buffers;
using BenchmarkDotNet.Attributes;
using Haqon.RocksDb.Extensions;
using Haqon.RocksDb.Serialization;
using Haqon.RocksDb.Tables;
using RocksDbSharp;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

namespace Benchmarks;

/*
| Method                                  | Mean       | Error    | StdDev    | Median     | Allocated |
|---------------------------------------- |-----------:|---------:|----------:|-----------:|----------:|
| PutRaw                                  | 3,732.6 ns | 73.77 ns | 125.26 ns | 3,673.6 ns |         - |
| PutRocksDbTable                         | 3,538.7 ns | 70.67 ns | 155.12 ns | 3,482.4 ns |         - |
| PutRocksDbTableWithConcurrentSupport    | 3,512.6 ns | 35.81 ns |  27.96 ns | 3,516.3 ns |         - |
| RemoveRaw                               | 3,219.8 ns | 15.57 ns |  13.00 ns | 3,215.8 ns |         - |
| RemoveRocksDbTable                      | 3,347.9 ns | 22.31 ns |  17.42 ns | 3,348.2 ns |         - |
| RemoveRocksDbTableWithConcurrentSupport | 3,368.7 ns | 34.91 ns |  29.15 ns | 3,359.1 ns |         - |
| GetRaw                                  |   152.8 ns |  0.74 ns |   0.58 ns |   152.6 ns |         - |
| GetRocksDbTable                         |   229.2 ns |  0.49 ns |   0.38 ns |   229.1 ns |         - |
*/
[MemoryDiagnoser()]
public class RawVsRocksDbTableBenchmarks
{
    private RocksDb _db;
    private string _path;

    private ColumnFamilyHandle _columnFamilyHandle;
    private IRocksDbTable<int, Customer> _simpleTable;
    private IRocksDbTable<int, Customer> _concurrentTable;
    private ArrayBufferWriter<byte> _keyWriter;
    private ArrayBufferWriter<byte> _valueWriter;
    private readonly Customer _customer = new(100, "John Doe");

    private const int RepeatCount = 1000;

    [GlobalSetup(Targets = [nameof(PutRaw), nameof(GetRaw), nameof(RemoveRaw)])]
    public void GlobalSetupRaw()
    {
        _db = TestHelper.CreateTempRocksDb(out _path);
        _columnFamilyHandle = _db.CreateColumnFamily(new ColumnFamilyOptions(), "name");

        _keyWriter = new ArrayBufferWriter<byte>(1024);
        _valueWriter = new ArrayBufferWriter<byte>(1024);
        PutRaw();
        GetRaw();
        RemoveRaw();
    }


    [GlobalSetup(Targets = [nameof(PutRocksDbTable), nameof(GetRocksDbTable), nameof(RemoveRocksDbTable)])]
    public void GlobalSetupRocksDbTable()
    {
        _db = TestHelper.CreateTempRocksDb(out _path);
        _simpleTable = _db.CreateTable(c => c.Id, Int32RockSerializer.Instance, CustomerSerializer.Instance);

        PutRocksDbTable();
        GetRocksDbTable();
        RemoveRocksDbTable();
    }

    [GlobalSetup(Targets = [nameof(PutRocksDbTableWithConcurrentSupport), nameof(RemoveRocksDbTableWithConcurrentSupport)])]
    public void GlobalSetupConcurrentRocksDbTable()
    {
        _db = TestHelper.CreateTempRocksDb(out _path);
        _concurrentTable = _db.CreateTable(c => c.Id, Int32RockSerializer.Instance, CustomerSerializer.Instance, opt => opt.SetEnableConcurrentChangesWithinRow(true));

        PutRocksDbTableWithConcurrentSupport();
        RemoveRocksDbTableWithConcurrentSupport();
    }

    [Benchmark(OperationsPerInvoke = RepeatCount)]
    public void PutRaw()
    {
        for (int i = 0; i < RepeatCount; i++)
        {
            Int32RockSerializer.Instance.Serialize(_keyWriter, _customer.Id);
            CustomerSerializer.Instance.Serialize(_valueWriter, _customer);
            _db.Put(_keyWriter.WrittenSpan, _valueWriter.WrittenSpan, _columnFamilyHandle);

            _keyWriter.ResetWrittenCount();
            _valueWriter.ResetWrittenCount();
        }
    }

    [Benchmark(OperationsPerInvoke = RepeatCount)]
    public void PutRocksDbTable()
    {
        for (int i = 0; i < RepeatCount; i++)
        {
            _simpleTable.Put(_customer);
        }
    }

    [Benchmark(OperationsPerInvoke = RepeatCount)]
    public void PutRocksDbTableWithConcurrentSupport()
    {
        for (int i = 0; i < RepeatCount; i++)
        {
            _concurrentTable.Put(_customer);
        }
    }

    [Benchmark(OperationsPerInvoke = RepeatCount)]
    public void RemoveRaw()
    {
        for (int i = 0; i < RepeatCount; i++)
        {
            Int32RockSerializer.Instance.Serialize(_keyWriter, _customer.Id);
            _db.Remove(_keyWriter.WrittenSpan, _columnFamilyHandle);
            _keyWriter.ResetWrittenCount();
        }
    }

    [Benchmark(OperationsPerInvoke = RepeatCount)]
    public void RemoveRocksDbTable()
    {
        for (int i = 0; i < RepeatCount; i++)
        {
            _simpleTable.Remove(_customer.Id);
        }
    }

    [Benchmark(OperationsPerInvoke = RepeatCount)]
    public void RemoveRocksDbTableWithConcurrentSupport()
    {
        for (int i = 0; i < RepeatCount; i++)
        {
            _concurrentTable.Remove(_customer.Id);
        }
    }

    [Benchmark(OperationsPerInvoke = RepeatCount)]
    public Customer? GetRaw()
    {
        Customer? result = null;

        for (int i = 0; i < RepeatCount; i++)
        {
            Int32RockSerializer.Instance.Serialize(_keyWriter, _customer.Id);
            result = _db.Get(_keyWriter.WrittenSpan, CustomerSpanDeserializer.Instance, _columnFamilyHandle);
            _keyWriter.ResetWrittenCount();
        }

        return result;
    }

    [Benchmark(OperationsPerInvoke = RepeatCount)]
    public Customer? GetRocksDbTable()
    {
        Customer? result = null;

        for (int i = 0; i < RepeatCount; i++)
        {
            result = _simpleTable.GetByKey(_customer.Id);
        }

        return result;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
        if (_db is not null)
        {
            _db.Dispose();
            Directory.Delete(_path, true);
        }

        _columnFamilyHandle = null!;
        _db = null!;
        _simpleTable = null!;
        _concurrentTable = null!;
        _keyWriter = null!;
        _valueWriter = null!;
    }
}

public record Customer(int Id, string Name);

public class CustomerSerializer : IRockSerializer<Customer>
{
    public static readonly CustomerSerializer Instance = new();

    public void Serialize(IBufferWriter<byte> writer, Customer value)
    {
        Int32RockSerializer.Instance.Serialize(writer, value.Id);
        StringRockSerializer.Utf8.Serialize(writer, value.Name);
    }

    public Customer Deserialize(ReadOnlySpan<byte> span)
    {
        var id = Int32RockSerializer.Instance.Deserialize(span.Slice(0, 4));
        var name = StringRockSerializer.Utf8.Deserialize(span.Slice(4));
        return new Customer(id, name);
    }
}

public class CustomerSpanDeserializer : ISpanDeserializer<Customer>
{
    public static readonly CustomerSpanDeserializer Instance = new();

    public Customer Deserialize(ReadOnlySpan<byte> buffer)
    {
        return CustomerSerializer.Instance.Deserialize(buffer);
    }
}