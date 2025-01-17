## Overview
`RocksDbTable` is a high-performance library for managing tables and indexes in a RocksDB database. Designed to provide flexible and efficient storage, retrieval, and manipulation of data, the library supports features like unique and non-unique indexes, transactions, and configurable options to tailor database operations to specific needs.

This library is perfect for developers looking to leverage the power of RocksDB while simplifying complex operations like index creation and transactional updates.
## Key Features

- **Primary Key and Index Support**: Define primary keys and create unique or non-unique indexes for advanced querying capabilities.
- **Transaction Support**: Execute multiple operations atomically using transactions.
- **Advanced Querying**: Retrieve entries by prefix, range, or exact match.
- **Customizable Indexes**: Configure indexes to store only references to values or full data.
- **High Performance**: Built on RocksDB, known for low-latency read and write operations.
- **Flexible Options**: Fine-tune column family and storage options to suit your requirements.

## Installation

Install the package via NuGet:

```bash
dotnet add package RocksDbTable
```

## Quick Start
Here’s a simple example to get started:

```csharp
using RocksDbTable;

// Define your primary key and value types
public record Student(int Id, string Name, string PassportId);

// Create RocksDb
var rocksDbPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
Directory.CreateDirectory(path);
var options = new DbOptions().SetCreateIfMissing(true);
var rocksDb = RocksDb.Open(options, path, new ColumnFamilies());

// Create Table And Indexes
var table = rocksDb.CreateTable(s => s.Id, Int32RockSerializer.Instance, StudentSerializer.Instance);
var nameIndex = table.CreateNotUniqueIndex(s => s.Name, StringRockSerializer.Utf8);
var passportIdIndex = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8);

// Add Data
var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
var putValue2 = new Student(200, "John Doe", "1111 2222 3333 4444");
var putValue3 = new Student(300, "John Snow", "4543 3241 4531 6124");

table.Put(putValue1);
table.Put(putValue2);
table.Put(putValue3);

// Get Data
var allTableValues = table.GetAllValues(); // returns: [putValue1, putValue2, putValue3]
var student200 = table.GetByKey(200); // returns: putValue2

var jonDoeStudents = nameIndex.GetAllValuesByKey("John Doe"); // returns: [putValue1, putValue2]
var studentByPassportId = passportIdIndex.GetByKey("1111 2222 3333 4444") // returns: putValue2

// Remove Data
table.Remove(100);
```

## Indexing
### Unique Index
You can create a unique index to ensure that keys are unique and allow efficient lookups.

```csharp
var passportIdIndex = table.CreateUniqueIndex(
        s => s.PassportId, // Unique key provider from row 
        StringRockSerializer.Utf8, // Unique key serializer (you can implement your own serializer by implementing the IRockSerializer interface)
        opt => opt.SetColumnFamilyName("PassportIdIndex") // Optional configuration
    );
```

### Non-Unique Index
Non-unique indexes allow you to associate multiple values with a single key.

```csharp
var fullNameIndex = table.CreateNotUniqueIndex(
        s => s.FullName, // Group by full name
        StringRockSerializer.Utf8, // Key serializer (you can implement your own serializer by implementing the IRockSerializer interface)
        opt => opt.SetColumnFamilyName("FullNameIndex") // Optional configuration
    );
```

### Reference-Only Indexes
By default, indexes store full values (rows), but you can configure them to store only references (primary key of the table).
```csharp
var index = table.CreateUniqueIndex(
    user => user.Id,
    new IntSerializer(),
    opts => opts.SetValueStoreMode(ValueStoreMode.Reference)
);
```
> **Note**: Storing references only reduces duplication but ensures that referenced values remain valid in the table.
> Storing a reference to a table in an index instead of a full copy of the row reduces data retrieval performance.

## Transactions
Transactions allow atomic updates across multiple operations.
```csharp
var putValue = new Student(100, "John Doe", "1729 1239 1239 9999");
var transaction = _rocksDb.CreateTransaction();

table.Put(putValue, ref transaction);
table.Remove(1337, ref transaction);
table.GetAllValues() // returns empty

transaction.Commit();
table.GetAllValues() // returns [putValue]
```

## Serialization

Implement your own data serializer to suit your needs.

For example, you can use the `MemoryPack` serializer like this.
```csharp
public class MemoryPackSerializer<T> : IRockSerializer<T>
{
    public static readonly MemoryPackSerializer<T> Default = new();

    private MemoryPackSerializer()
    {
    }

    public void Serialize(IBufferWriter<byte> writer, T value)
    {
        MemoryPackSerializer.Serialize(writer, value);
    }

    public T Deserialize(ReadOnlySpan<byte> span)
    {
        return MemoryPackSerializer.Deserialize<T>(span)!;
    }
}
```

`SystemTextJson` serializer example:
```csharp
public class SystemTextJsonRockSerializer<T> : IRockSerializer<T>
{
    public void Serialize(IBufferWriter<byte> writer, T value)
    {
        var jsonWriter = new Utf8JsonWriter(writer);
        JsonSerializer.Serialize(jsonWriter, value);
    }

    public T Deserialize(ReadOnlySpan<byte> span)
    {
        return JsonSerializer.Deserialize<T>(span)!;
    }
}
```

## Data retrieve
#### **IRocksDbTable**
- `IEnumerable<TValue> GetAllValues()`: Retrieves all values (entries) in the table.
- `TValue? GetByKey(TPrimaryKey key)`: Retrieves a value associated with the specified unique key.
- `bool HasExactKey(TPrimaryKey key)`: Checks if the store contains an exact match for the specified key.
- `IEnumerable<TUniqueKey> GetAllKeys()`: Retrieves all primary keys in the store.
- `IEnumerable<TUniqueKey> GetAllKeysByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer)`: Retrieves all keys that match the specified prefix.


#### **IUniqueIndex**
- `TValue? GetByKey(TUniqueKey uniqueKey)`: Retrieves a value associated with the specified unique key.
- `bool HasExactKey(TUniqueKey uniqueKey)`: Checks if the store contains an exact match for the specified key.
- `IEnumerable<TUniqueKey> GetAllKeys()`: Retrieves all unique keys in the store.
- `IEnumerable<TUniqueKey> GetAllKeysByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer)`: Retrieves all keys that match the specified prefix.
- `TValue? GetFirstValueByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> serializer, SeekMode mode = SeekMode.SeekToFirst)`: Retrieves the first value that matches the specified key prefix.
- `IEnumerable<TValue> GetAllValuesByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer)`: Retrieves all values that match the specified key prefix.
- `IEnumerable<TValue> GetAllValuesByBounds(TKey startInclusive, TKey endExclusive)`: Retrieves all values within the specified range of keys.
- `bool HasAnyKeyByPrefix<TPrefixKey>(TPrefixKey key, IRockSerializer<TPrefixKey> serializer)`: Checks if the store contains any key that matches the specified prefix.


#### **INotUniqueIndex**
- `bool HasAny(TNotUniqueKey key)`: Determines whether any values are associated with the specified key.
- `TValue? GetFirstValue(TNotUniqueKey key, SeekMode mode = SeekMode.SeekToFirst)`: Retrieves the first value associated with the specified key.
- `IEnumerable<TValue> GetAllValuesByKey(TNotUniqueKey key)`: Retrieves all values associated with the specified key.
- `TValue? GetFirstValueByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> serializer, SeekMode mode = SeekMode.SeekToFirst)`: Retrieves the first value that matches the specified key prefix.
- `IEnumerable<TValue> GetAllValuesByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer)`: Retrieves all values that match the specified key prefix.
- `IEnumerable<TValue> GetAllValuesByBounds(TKey startInclusive, TKey endExclusive)`: Retrieves all values within the specified range of keys.
- `bool HasAnyKeyByPrefix<TPrefixKey>(TPrefixKey key, IRockSerializer<TPrefixKey> serializer)`: Checks if the store contains any key that matches the specified prefix.

## Concurrency
By default, concurrent modifications of the same entry can lead to conflicts. Call `SetEnableConcurrentChangesWithinRow` on the table configuration to resolve issues with concurrent row modifications.

## Change tracker
Through the table options, you can define a changes consumer that reacts to events such as adding, updating, or deleting records.
```csharp
var consumer = new ExampleConsumer();
var table = rocksDb.CreateTable<int, Student>(
    s => s.Id, 
    Int32RockSerializer.Instance, 
    StudentSerializer.Instance, 
    opt => opt.SetTableChangesConsumer(consumer)
);

public class ExampleConsumer : IRocksDbTableChangesConsumer<int, Student>
{
    public void Removed(int key, Student removedValue)
    {
        Console.WriteLine($"Student removed: {key}");
    }

    public void AddedOrUpdated(int key, Student? oldValue, Student newValue)
    {
        if (oldValue is null)
        {
            Console.WriteLine($"Student added: {key}");
        }
        else
        {
            Console.WriteLine($"Student updated: {oldValue.Name} => {newValue.Name}");
        }
    }
}
```

## License
This library is licensed under the MIT License.