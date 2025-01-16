using FluentAssertions;
using Haqon.RocksDb.Core;
using Haqon.RocksDb.Options;
using Haqon.RocksDb.Serialization;
using Haqon.RocksDb.Utils;
using Moq;
using RocksDbSharp;
using Tests.Infrastructure;

namespace Tests;

public class KeyValueStoreBaseTests : IDisposable
{
    private readonly Mock<KeyValueStoreBase<string, int>> _mockKeyValueStoreBase;
    private readonly string _rocksDbPath;
    private readonly RocksDb _rocksDb;
    private readonly IRockSerializer<string> _keySerializer;
    private readonly IRockSerializer<int> _valueSerializer;
    private readonly Mock<IStoreOptions> _storeOptions;

    public KeyValueStoreBaseTests()
    {
        _keySerializer = StringRockSerializer.Utf8;
        _valueSerializer = Int32RockSerializer.Instance;

        _storeOptions = new Mock<IStoreOptions>();
        _storeOptions.SetupGet(x => x.ColumnFamilyName).Returns("TestName");
        _storeOptions.SetupGet(x => x.ColumnFamilyOptions).Returns(new ColumnFamilyOptions());

        _rocksDb = TestHelper.CreateTempRocksDb(out _rocksDbPath);
        _mockKeyValueStoreBase = new Mock<KeyValueStoreBase<string, int>>(_rocksDb, _keySerializer, _storeOptions.Object, new RocksDbSpanDeserializerAdapter<int>(_valueSerializer))
        {
            CallBase = true
        };
    }

    #region GetByKeySpan

    [Fact]
    public void GetByKeySpan_KeyExists_ShouldReturnValue()
    {
        // Arrange
        var testKey = "Key1";
        var testValue = 5;
        Put("Key0", 1);
        Put(testKey, testValue);
        Put("Key5", 100);

        // Act
        var keyBytes = Serialize(_keySerializer, testKey);
        var result = _mockKeyValueStoreBase.Object.GetByKey(keyBytes);

        // Assert
        result.Should().Be(testValue);
    }

    [Fact]
    public void GetByKeySpan_KeyDoesNotExist_ShouldReturnDefault()
    {
        // Arrange
        var testKey = "Key1";

        // Act
        var keyBytes = Serialize(_keySerializer, testKey);
        var result = _mockKeyValueStoreBase.Object.GetByKey(keyBytes);

        // Assert
        result.Should().Be(0);
    }

    #endregion

    #region GetByKey

    [Fact]
    public void GetByKey_KeyExists_ShouldReturnValue()
    {
        // Arrange
        var testKey = "Key1";
        var testValue = 5;
        Put("Key0", 1);
        Put(testKey, testValue);
        Put("Key5", 100);

        // Act
        var result = _mockKeyValueStoreBase.Object.GetByKey(testKey);

        // Assert
        result.Should().Be(testValue);
    }

    [Fact]
    public void GetByKey_KeyDoesNotExist_ShouldReturnDefault()
    {
        // Arrange
        var testKey = "Key1";

        // Act
        var result = _mockKeyValueStoreBase.Object.GetByKey(testKey);

        // Assert
        result.Should().Be(0);
    }

    #endregion

    #region HasExactKey

    [Fact]
    public void HasExactKey_KeyExists_ShouldReturnTrue()
    {
        // Arrange
        var testKey = "Key1";
        var testValue = 5;

        Put(testKey, testValue);
        Put("Key11", 100);

        // Act
        var result = _mockKeyValueStoreBase.Object.HasExactKey(testKey);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void HasExactKey_KeyDoesNotExist_ShouldReturnFalse()
    {
        // Arrange
        Put("Key11", 100);

        // Act
        var result = _mockKeyValueStoreBase.Object.HasExactKey("Key1");

        // Assert
        result.Should().BeFalse();
    }

    #endregion

    #region HasAnyKeyByPrefix

    [Fact]
    public void HasAnyKeyByPrefix_KeyWithPrefixExists_ShouldReturnTrue()
    {
        // Arrange
        Put("Key11", 100);

        // Act
        var result = _mockKeyValueStoreBase.Object.HasAnyKeyByPrefix("Key1", _keySerializer);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void HasAnyKeyByPrefix_KeyWithPrefixDoesNotExist_ShouldReturnFalse()
    {
        // Arrange
        Put("Key", 100);
        Put("Key22", 100);
        Put("Key33", 100);

        // Act
        var result = _mockKeyValueStoreBase.Object.HasExactKey("Key1");

        // Assert
        result.Should().BeFalse();
    }

    #endregion

    #region GetAllKeys

    [Fact]
    public void GetAllKeys_ShouldReturnAllKeys()
    {
        // Arrange
        const int count = 10;
        var testRows = Enumerable.Range(0, count)
            .Select(i => new KeyValuePair<string, int>(i.ToString(), i))
            .ToArray();

        foreach (var testRow in testRows)
        {
            Put(testRow.Key, testRow.Value);
        }

        // Act
        var result = new List<string>(_mockKeyValueStoreBase.Object.GetAllKeys());

        // Assert
        result.Should().HaveCount(count);
        result.Should().BeEquivalentTo(testRows.Select(r => r.Key));
    }

    #endregion

    #region GetAllValues

    [Fact]
    public void GetAllValues_ShouldReturnAllValues()
    {
        // Arrange
        const int count = 10;
        var testRows = Enumerable.Range(0, count)
            .Select(i => new KeyValuePair<string, int>(i.ToString(), i))
            .ToArray();

        foreach (var testRow in testRows)
        {
            Put(testRow.Key, testRow.Value);
        }

        // Act
        var result = new List<int>(_mockKeyValueStoreBase.Object.GetAllValues());

        // Assert
        result.Should().HaveCount(count);
        result.Should().BeEquivalentTo(testRows.Select(r => r.Value));
    }

    #endregion

    #region GetAllKeysByPrefix

    [Fact]
    public void GetAllKeysByPrefix_ShouldReturnKeysWithPrefix()
    {
        // Arrange
        const string prefix = "SomePrefix_";
        const int prefixCount = 10;

        var beforeRows = Enumerable.Range(0, 10)
            .Select(i => new KeyValuePair<string, int>("A" + i.ToString(), i))
            .ToArray();

        var prefixRows = Enumerable.Range(0, prefixCount)
            .Select(i => new KeyValuePair<string, int>(prefix + i, i))
            .ToArray();

        var rowsAfter = Enumerable.Range(0, 10)
            .Select(i => new KeyValuePair<string, int>("Z" + i, i))
            .ToArray();

        foreach (var testRow in beforeRows.Concat(prefixRows).Concat(rowsAfter))
        {
            Put(testRow.Key, testRow.Value);
        }

        // Act
        var result = new List<string>(_mockKeyValueStoreBase.Object.GetAllKeysByPrefix(prefix, _keySerializer));

        // Assert
        result.Should().HaveCount(prefixCount);
        result.Should().BeEquivalentTo(prefixRows.Select(r => r.Key));
    }

    #endregion

    #region GetAllValuesByPrefix

    [Fact]
    public void GetAllValuesByPrefix_ShouldReturnValuesWithPrefixKey()
    {
        // Arrange
        const string prefix = "SomePrefix_";
        const int prefixCount = 10;

        var beforeRows = Enumerable.Range(0, 10)
            .Select(i => new KeyValuePair<string, int>("A" + i.ToString(), i))
            .ToArray();

        var prefixRows = Enumerable.Range(0, prefixCount)
            .Select(i => new KeyValuePair<string, int>(prefix + i, i + beforeRows.Length))
            .ToArray();

        var rowsAfter = Enumerable.Range(0, 10)
            .Select(i => new KeyValuePair<string, int>("Z" + i, i + beforeRows.Length + prefixRows.Length))
            .ToArray();

        foreach (var testRow in beforeRows.Concat(prefixRows).Concat(rowsAfter))
        {
            Put(testRow.Key, testRow.Value);
        }

        // Act
        var result = new List<int>(_mockKeyValueStoreBase.Object.GetAllValuesByPrefix(prefix, _keySerializer));

        // Assert
        result.Should().HaveCount(prefixCount);
        result.Should().BeEquivalentTo(prefixRows.Select(r => r.Value));
    }

    #endregion

    #region GetFirstValueByPrefix

    [Fact]
    public void GetFirstValueByPrefix_SeekToFirst_ShouldReturnValue()
    {
        // Arrange
        const string prefix = "SomePrefix_";
        const int prefixCount = 10;

        var beforeRows = Enumerable.Range(0, 10)
            .Select(i => new KeyValuePair<string, int>("A" + i.ToString(), i))
            .ToArray();

        var prefixRows = Enumerable.Range(0, prefixCount)
            .Select(i => new KeyValuePair<string, int>(prefix + i, i + beforeRows.Length))
            .ToArray();

        var rowsAfter = Enumerable.Range(0, 10)
            .Select(i => new KeyValuePair<string, int>("Z" + i, i + beforeRows.Length + prefixRows.Length))
            .ToArray();

        foreach (var testRow in beforeRows.Concat(prefixRows).Concat(rowsAfter))
        {
            Put(testRow.Key, testRow.Value);
        }

        // Act
        var result = _mockKeyValueStoreBase.Object.GetFirstValueByPrefix(prefix, _keySerializer, SeekMode.SeekToFirst);

        // Assert
        result.Should().Be(prefixRows.First().Value);
    }

    [Fact]
    public void GetFirstValueByPrefix_SeekToFirst_ShouldReturnDefaultIfNotValueByPrefix()
    {
        // Arrange
        const string prefix = "SomePrefix_";
        const int prefixCount = 10;

        var beforeRows = Enumerable.Range(0, 10)
            .Select(i => new KeyValuePair<string, int>("A" + i.ToString(), i))
            .ToArray();

        var prefixRows = Enumerable.Range(0, prefixCount)
            .Select(i => new KeyValuePair<string, int>(prefix + i, i + beforeRows.Length))
            .ToArray();

        var rowsAfter = Enumerable.Range(0, 10)
            .Select(i => new KeyValuePair<string, int>("Z" + i, i + beforeRows.Length + prefixRows.Length))
            .ToArray();

        foreach (var testRow in beforeRows.Concat(prefixRows).Concat(rowsAfter))
        {
            Put(testRow.Key, testRow.Value);
        }

        // Act
        var result = _mockKeyValueStoreBase.Object.GetFirstValueByPrefix("AnotherPrefix", _keySerializer, SeekMode.SeekToFirst);

        // Assert
        result.Should().Be(0);
    }

    [Fact]
    public void GetFirstValueByPrefix_SeekToPrev_ShouldReturnValue()
    {
        // Arrange
        const string prefix = "SomePrefix_";
        const int prefixCount = 5;

        var beforeRows = Enumerable.Range(0, 3)
            .Select(i => new KeyValuePair<string, int>("A" + i.ToString(), i))
            .ToArray();

        var prefixRows = Enumerable.Range(0, prefixCount)
            .Select(i => new KeyValuePair<string, int>(prefix + i, i + beforeRows.Length))
            .ToArray();

        var rowsAfter = Enumerable.Range(0, 3)
            .Select(i => new KeyValuePair<string, int>("Z" + i, i + beforeRows.Length + prefixRows.Length))
            .ToArray();

        foreach (var testRow in beforeRows.Concat(prefixRows).Concat(rowsAfter))
        {
            Put(testRow.Key, testRow.Value);
        }

        // Act
        var result = _mockKeyValueStoreBase.Object.GetFirstValueByPrefix(prefix + (prefixRows.Last().Value + 1), _keySerializer, SeekMode.SeekToPrev);

        // Assert
        result.Should().Be(prefixRows.Last().Value);
    }

    [Fact]
    public void GetFirstValueByPrefix_SeekToPrev_ShouldReturnDefaultIfNotValueByPrefix()
    {
        // Arrange
        const string prefix = "SomePrefix_";
        const int prefixCount = 5;

        var beforeRows = Enumerable.Range(0, 3)
            .Select(i => new KeyValuePair<string, int>("A" + i.ToString(), i + 1))
            .ToArray();

        var prefixRows = Enumerable.Range(0, prefixCount)
            .Select(i => new KeyValuePair<string, int>(prefix + i, i + beforeRows.Last().Value + 1))
            .ToArray();

        var rowsAfter = Enumerable.Range(0, 3)
            .Select(i => new KeyValuePair<string, int>("X" + i, i + prefixRows.Last().Value + 1))
            .ToArray();

        foreach (var testRow in beforeRows.Concat(prefixRows).Concat(rowsAfter))
        {
            Put(testRow.Key, testRow.Value);
        }

        // Act
        var result = _mockKeyValueStoreBase.Object.GetFirstValueByPrefix("A", _keySerializer, SeekMode.SeekToPrev);

        // Assert
        result.Should().Be(default);
    }

    #endregion

    #region GetAllValuesByBounds

    [Fact]
    public void GetAllValuesByBounds_ShouldReturnValuesInRange()
    {
        // Arrange
        const string prefix = "SomePrefix_";
        const int prefixCount = 5;

        var beforeRows = Enumerable.Range(0, 3)
            .Select(i => new KeyValuePair<string, int>("A" + i.ToString(), i + 1))
            .ToArray();

        // 4 5 6 7 8
        var prefixRows = Enumerable.Range(0, prefixCount)
            .Select(i => new KeyValuePair<string, int>(prefix + i, i + beforeRows.Last().Value + 1))
            .ToArray();

        var rowsAfter = Enumerable.Range(0, 3)
            .Select(i => new KeyValuePair<string, int>("X" + i, i + prefixRows.Last().Value + 1))
            .ToArray();

        foreach (var testRow in beforeRows.Concat(prefixRows).Concat(rowsAfter))
        {
            Put(testRow.Key, testRow.Value);
        }

        // Act
        var result = new List<int>(_mockKeyValueStoreBase.Object.GetAllValuesByBounds(prefixRows[1].Key, prefixRows[^1].Key));

        // Assert
        result.Should().BeEquivalentTo(prefixRows.Skip(1).SkipLast(1).Select(r => r.Value));
    }

    [Fact]
    public void GetAllValuesByBounds_ShouldReturnEmptyIfNoKeysInRange()
    {
        // Arrange
        const string prefix = "SomePrefix_";
        const int prefixCount = 5;

        var beforeRows = Enumerable.Range(0, 3)
            .Select(i => new KeyValuePair<string, int>("A" + i.ToString(), i + 1))
            .ToArray();

        // 4 5 6 7 8
        var prefixRows = Enumerable.Range(0, prefixCount)
            .Select(i => new KeyValuePair<string, int>(prefix + i, i + beforeRows.Last().Value + 1))
            .ToArray();

        var rowsAfter = Enumerable.Range(0, 3)
            .Select(i => new KeyValuePair<string, int>("X" + i, i + prefixRows.Last().Value + 1))
            .ToArray();

        foreach (var testRow in beforeRows.Concat(prefixRows).Concat(rowsAfter))
        {
            Put(testRow.Key, testRow.Value);
        }

        // Act
        var result = new List<int>(_mockKeyValueStoreBase.Object.GetAllValuesByBounds("Z", "ZZZ"));

        // Assert
        result.Should().BeEmpty();
    }

    #endregion

    private void Put(string key, int value)
    {
        _rocksDb.Put(Serialize(_keySerializer, key), Serialize(_valueSerializer, value), _mockKeyValueStoreBase.Object.ColumnFamilyHandle);
    }

    private static byte[] Serialize<T>(IRockSerializer<T> serializer, T value)
    {
        var writer = new ArrayPoolBufferWriter();
        try
        {
            serializer.Serialize(writer, value);
            return writer.WrittenSpan.ToArray();
        }
        finally
        {
            writer.Dispose();
        }
    }

    public void Dispose()
    {
        _rocksDb.Dispose();
        Directory.Delete(_rocksDbPath, true);
    }
}