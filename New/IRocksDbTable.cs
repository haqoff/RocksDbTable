using System;
using System.Collections.Generic;
using RocksDbSharp;

namespace Haqon.RocksDb.New;

public interface IRocksDbTable<in TPrimaryKey, TValue>
{
    void Remove<TWrapper>(TPrimaryKey primaryKey, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper;
    void Put<TWrapper>(TValue value, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper;
    TValue? GetByKey(ReadOnlySpan<byte> buffer);
}

public class RocksDbTable<TPrimaryKey, TValue> : IRocksDbTable<TPrimaryKey, TValue>
{
    private readonly RocksDbSharp.RocksDb _rocksDb;
    private readonly Func<TValue, TPrimaryKey> _keyProvider;
    private readonly IRockSerializer<TPrimaryKey> _primaryKeySerializer;
    private readonly IRockSerializer<TValue> _valueSerializer;
    private readonly TableOptions _tableOptions;
    private readonly ColumnFamilyHandle _columnFamilyHandle;
    private readonly RocksDeserializerAdapter<TValue> _valueDeserializer;

    private readonly List<IDependentIndex<TValue>> _dependentIndexes = new();

    public RocksDbTable(RocksDbSharp.RocksDb rocksDb, Func<TValue, TPrimaryKey> keyProvider, IRockSerializer<TPrimaryKey> primaryKeySerializer, IRockSerializer<TValue> valueSerializer, TableOptions tableOptions)
    {
        _rocksDb = rocksDb;
        _keyProvider = keyProvider;
        _primaryKeySerializer = primaryKeySerializer;
        _valueSerializer = valueSerializer;
        _tableOptions = tableOptions;
        _columnFamilyHandle = rocksDb.CreateColumnFamily(tableOptions.ColumnFamilyOptions, tableOptions.ColumnFamilyName);
        _valueDeserializer = new RocksDeserializerAdapter<TValue>(valueSerializer);
    }

    #region Get

    /// <summary>
    /// На основе указанного значения <paramref name="value"/> вычисляет ключ и получает значение из хранилища. 
    /// </summary>
    public TValue? GetByValue(TValue value)
    {
        var key = _keyProvider(value);
        return GetByKey(key);
    }

    /// <summary>
    /// Получает значение по первичному ключу.
    /// </summary>
    public TValue? GetByKey(TPrimaryKey key)
    {
        var keyBuffer = new ArrayPoolBufferWriter();
        try
        {
            _primaryKeySerializer.Serialize(ref keyBuffer, key);
            return GetByKey(keyBuffer.WrittenSpan);
        }
        finally
        {
            keyBuffer.Dispose();
        }
    }

    public TValue? GetByKey(ReadOnlySpan<byte> key)
    {
        return _rocksDb.Get(key, _valueDeserializer, _columnFamilyHandle);
    }


    public bool HasKey(in TPrimaryKey uniqueKey)
    {
        var keyBuffer = new ArrayPoolBufferWriter();
        try
        {
            _primaryKeySerializer.Serialize(ref keyBuffer, in uniqueKey);
            return _rocksDb.HasKey(keyBuffer.WrittenSpan, _columnFamilyHandle);
        }
        finally
        {
            keyBuffer.Dispose();
        }
    }

    public IEnumerable<TPrimaryKey> GetAllKeys()
    {
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle);
            for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
            {
                var fullKey = _primaryKeySerializer.Deserialize(iterator.GetKeySpan())!;
                yield return fullKey;
            }
        }
        finally
        {
            buffer.Dispose();
        }
    }

    public TValue? GetFirstValueByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> serializer, SeekMode mode = SeekMode.SeekToFirst)
    {
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            serializer.Serialize(ref buffer, in prefixKey);

            if (mode == SeekMode.SeekToPrev)
            {
                iterator.SeekForPrev(buffer.WrittenSpan);
            }
            else
            {
                iterator.Seek(buffer.WrittenSpan);
            }

            if (!iterator.Valid())
            {
                return default;
            }

            var value = _valueDeserializer.Deserialize(iterator.GetValueSpan());
            return value;
        }
        finally
        {
            buffer.Dispose();
        }
    }

    public IEnumerable<TPrimaryKey> GetAllKeysByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer)
    {
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            prefixSerializer.Serialize(ref buffer, in prefixKey);
            for (iterator.Seek(buffer.WrittenSpan); iterator.Valid(); iterator.Next())
            {
                var fullKey = _primaryKeySerializer.Deserialize(iterator.GetKeySpan())!;
                yield return fullKey;
            }
        }
        finally
        {
            buffer.Dispose();
        }
    }

    public IEnumerable<TValue> GetAllValuesByPrefix<TPrefixKey>(TPrefixKey prefixKey, IRockSerializer<TPrefixKey> prefixSerializer)
    {
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = _rocksDb.NewIterator(_columnFamilyHandle, SharedReadOptions.OnlyPrefixRead);
            prefixSerializer.Serialize(ref buffer, in prefixKey);
            for (iterator.Seek(buffer.WrittenSpan); iterator.Valid(); iterator.Next())
            {
                yield return _valueDeserializer.Deserialize(iterator.GetValueSpan())!;
            }
        }
        finally
        {
            buffer.Dispose();
        }
    }

    #endregion


    public void Remove<TWrapper>(TPrimaryKey primaryKey, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        var bufferWriter = new ArrayPoolBufferWriter();

        try
        {
            _primaryKeySerializer.Serialize(ref bufferWriter, in primaryKey);

            if (_dependentIndexes.Count > 0)
            {
                var currentValue = _rocksDb.Get(bufferWriter.WrittenSpan, _valueDeserializer, _columnFamilyHandle);
                if (currentValue is null)
                {
                    return;
                }

                foreach (var dependentIndex in _dependentIndexes)
                {
                    dependentIndex.Remove(bufferWriter.WrittenSpan, currentValue, ref transaction);
                }
            }

            transaction.CommandWrapper.Delete(bufferWriter.WrittenSpan, _columnFamilyHandle);
        }
        finally
        {
            bufferWriter.Dispose();
        }
    }

    public void Put<TWrapper>(TValue newValue, ref ChangeTransaction<TWrapper> transaction) where TWrapper : IRocksDbCommandWrapper
    {
        var key = _keyProvider(newValue);

        var keyBufferWriter = new ArrayPoolBufferWriter();
        var valueBufferWriter = new ArrayPoolBufferWriter();
        try
        {
            _primaryKeySerializer.Serialize(ref keyBufferWriter, in key);
            _valueSerializer.Serialize(ref valueBufferWriter, in newValue);
            if (_dependentIndexes.Count > 0)
            {
                var oldValue = _rocksDb.Get(keyBufferWriter.WrittenSpan, _valueDeserializer, _columnFamilyHandle);
                foreach (var dependentIndex in _dependentIndexes)
                {
                    dependentIndex.Put(keyBufferWriter.WrittenSpan, valueBufferWriter.WrittenSpan, oldValue, newValue, ref transaction);
                }
            }

            transaction.CommandWrapper.Put(keyBufferWriter.WrittenSpan, valueBufferWriter.WrittenSpan, _columnFamilyHandle);
        }
        finally
        {
            keyBufferWriter.Dispose();
            valueBufferWriter.Dispose();
        }
    }

    public IUniqueIndex<TUniqueKey, TValue> CreateUniqueIndex<TUniqueKey>(Func<TValue, TUniqueKey> uniqueKeyProvider, IRockSerializer<TUniqueKey> keySerializer, Action<IndexOptions>? configureAction = null)
    {
        var indexOptions = new IndexOptions();
        configureAction?.Invoke(indexOptions);
        var index = new UniqueIndex<TUniqueKey, TPrimaryKey, TValue>(_rocksDb, uniqueKeyProvider, keySerializer, _valueSerializer, this, indexOptions);
        _dependentIndexes.Add(index);
        return index;
    }

    public INotUniqueIndex<TNotUniqueKey, TValue> CreateNotUniqueIndex<TNotUniqueKey>(Func<TValue, TNotUniqueKey> notUniqueKeyProvider, IRockSerializer<TNotUniqueKey> keySerializer, Action<IndexOptions>? configureAction = null)
    {
        var indexOptions = new IndexOptions();
        configureAction?.Invoke(indexOptions);
        var index = new NotUniqueIndex<TNotUniqueKey, TPrimaryKey, TValue>(_rocksDb, notUniqueKeyProvider, keySerializer, _valueSerializer, this, indexOptions);
        _dependentIndexes.Add(index);
        return index;
    }
}

/*
internal abstract class TreeBase<TKey, TValue>
{
    protected RocksDbSharp.RocksDb RocksDb { get; }
    protected Func<TValue, TKey> KeyProvider { get; }
    protected ColumnFamilyOptions FamilyOptions { get; }
    protected IRockSerializer<TKey> KeySerializer { get; }
    protected ColumnFamilyHandle FamilyHandle { get; }
    protected RocksDeserializerAdapter<TValue> ValueDeserializer { get; }

    protected TreeBase(RocksDbSharp.RocksDb rocksDb, Func<TValue, TKey> keyProvider, ColumnFamilyOptions familyOptions, IRockSerializer<TKey> keySerializer, IRockSerializer<TValue> ValueSerializer, ColumnFamilyHandle familyHandle, RocksDeserializerAdapter<TValue> valueDeserializer)
    {
        RocksDb = rocksDb;
        KeyProvider = keyProvider;
        FamilyOptions = familyOptions;
        KeySerializer = keySerializer;
        FamilyHandle = familyHandle;
        ValueDeserializer = valueDeserializer;
    }

    #region Get

    protected TValue? GetByValue(TValue value)
    {
        var key = KeyProvider(value);
        return GetByKey(key);
    }

    protected TValue? GetByKey(TKey key)
    {
        var keyBuffer = new ArrayPoolBufferWriter();
        try
        {
            KeySerializer.Serialize(ref keyBuffer, in key);
            return GetByKey(keyBuffer.WrittenSpan);
        }
        finally
        {
            keyBuffer.Dispose();
        }
    }

    protected TValue? GetByKey(ReadOnlySpan<byte> key)
    {
        return RocksDb.Get(key, ValueDeserializer, FamilyHandle);
    }

    protected IEnumerable<TValue> GetAllValuesByPrefix<TPrefixKey>(TPrefixKey prefixKey, Func<TKey, TPrefixKey, bool> isValidPrefix, IRockSerializer<TPrefixKey> serializer)
    {
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = RocksDb.NewIterator(FamilyHandle);
            serializer.Serialize(ref buffer, in prefixKey);
            for (iterator.Seek(buffer.WrittenSpan); iterator.Valid(); iterator.Next())
            {
                var fullKey = KeySerializer.Deserialize(iterator.GetKeySpan())!;
                if (!isValidPrefix(fullKey, prefixKey))
                {
                    break;
                }

                var value = ValueDeserializer.Deserialize(iterator.GetValueSpan())!;
                yield return value;
            }
        }
        finally
        {
            buffer.Dispose();
        }
    }

    protected TValue? GetFirstValueByPrefix<TPrefixKey>(TPrefixKey prefixKey, Func<TKey, TPrefixKey, bool> isValidPrefix, IRockSerializer<TPrefixKey> serializer, SeekMode mode = SeekMode.SeekToFirst)
    {
        var buffer = new ArrayPoolBufferWriter();
        try
        {
            using var iterator = RocksDb.NewIterator(FamilyHandle);
            serializer.Serialize(ref buffer, in prefixKey);

            if (mode == SeekMode.SeekToPrev)
            {
                iterator.SeekForPrev(buffer.WrittenSpan);
            }
            else
            {
                iterator.Seek(buffer.WrittenSpan);
            }

            if (!iterator.Valid())
            {
                return default;
            }

            var fullKey = KeySerializer.Deserialize(iterator.GetKeySpan())!;
            if (!isValidPrefix(fullKey, prefixKey))
            {
                return default;
            }

            var value = ValueDeserializer.Deserialize(iterator.GetValueSpan())!;
            return value;
        }
        finally
        {
            buffer.Dispose();
        }
    }

    /// <summary>
    /// Извлекает все первичные ключи по префиксу.
    /// </summary>
    /// <param name="prefixKey">Префиксный ключ. Должен иметь общую префиксную часть с первичным ключом <typeparamref name="TKey"/>.</param>
    /// <param name="isValidPrefix">Предикат, проверяющий что префикс валиден для первичного ключа.</param>
    protected IEnumerable<TKey> GetAllKeysByPrefix<TPrefixKey>(TPrefixKey prefixKey, Func<TKey, TPrefixKey, bool> isValidPrefix)
    {
        using var buffer = new ArrayPoolBufferWriter();
        using var iterator = _rocksDb.NewIterator(_columnFamily.Handle);
        MemoryPackSerializer.Serialize(in buffer, prefixKey);
        for (iterator.Seek(buffer.WrittenSpan); iterator.Valid(); iterator.Next())
        {
            var fullKey = MemoryPackSerializer.Deserialize<TKey>(iterator.GetKeySpan())!;
            if (!isValidPrefix(fullKey, prefixKey))
            {
                break;
            }

            yield return fullKey;
        }
    }

    /// <summary>
    /// Получает все ключи в хранилище.
    /// </summary>
    protected IEnumerable<TKey> GetAllKeys()
    {
        using var iterator = _rocksDb.NewIterator(_columnFamily.Handle);
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
        {
            var fullKey = MemoryPackSerializer.Deserialize<TKey>(iterator.GetKeySpan())!;
            yield return fullKey;
        }
    }

    #endregion
}
*/