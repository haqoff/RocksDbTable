using System;
using System.Collections.Generic;
using MemoryPack;

namespace Haqon.RocksDb;

/// <summary>
/// Представляет собой дерево RocksDb.
/// Высокоуровневый интерфейс для работы с RocksDb + MemoryPack.
/// </summary>
/// <typeparam name="TKey">Тип первичного ключа.</typeparam>
/// <typeparam name="TValue">Тип значения.</typeparam>
public class RocksDbTree<TKey, TValue>
{
    private readonly RocksDbSharp.RocksDb _rocksDb;
    private readonly ColumnFamily _columnFamily;
    private readonly Func<TValue, TKey> _keyProvider;
    private readonly IChangeConsumer<TKey, TValue>? _changeConsumer = null!;

    public RocksDbTree(RocksDbSharp.RocksDb rocksDb, ColumnFamily columnFamily, Func<TValue, TKey> keyProvider)
    {
        _rocksDb = rocksDb;
        _columnFamily = columnFamily;
        _keyProvider = keyProvider;
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
    public TValue? GetByKey(TKey key)
    {
        using var keyBuffer = new ArrayPoolBufferWriter();
        MemoryPackSerializer.Serialize(in keyBuffer, in key);
        return GetByKey(keyBuffer.WrittenSpan);
    }

    private TValue? GetByKey(ReadOnlySpan<byte> key)
    {
        return _rocksDb.Get(key, MemoryPackRocksDbDeserializer<TValue>.Default, _columnFamily.Handle);
    }

    /// <summary>
    /// Извлекает все значения по префиксу первичного ключа.
    /// </summary>
    /// <param name="prefixKey">Префиксный ключ. Должен иметь общую префиксную часть с первичным ключом <typeparamref name="TKey"/>.</param>
    /// <param name="isValidPrefix">Предикат, проверяющий что префикс валиден для первичного ключа.</param>
    public IEnumerable<TValue> GetAllValuesByPrefix<TPrefixKey>(TPrefixKey prefixKey, Func<TKey, TPrefixKey, bool> isValidPrefix)
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

            var value = MemoryPackSerializer.Deserialize<TValue>(iterator.GetValueSpan())!;
            yield return value;
        }
    }

    /// <summary>
    /// Получает первое вхождение по префиксному ключу.
    /// </summary>
    /// <param name="prefixKey">Префиксный ключ. Должен иметь общую префиксную часть с первичным ключом <typeparamref name="TKey"/>.</param>
    /// <param name="isValidPrefix">Предикат, проверяющий что префикс валиден для первичного ключа.</param>
    /// <param name="mode">Тип поиска.</param>
    public TValue? GetFirstValueByPrefix<TPrefixKey>(TPrefixKey prefixKey, Func<TKey, TPrefixKey, bool> isValidPrefix, SeekMode mode = SeekMode.SeekToFirst)
    {
        using var buffer = new ArrayPoolBufferWriter();
        using var iterator = _rocksDb.NewIterator(_columnFamily.Handle);
        MemoryPackSerializer.Serialize(in buffer, prefixKey);

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

        var fullKey = MemoryPackSerializer.Deserialize<TKey>(iterator.GetKeySpan())!;
        if (!isValidPrefix(fullKey, prefixKey))
        {
            return default;
        }

        var value = MemoryPackSerializer.Deserialize<TValue>(iterator.GetValueSpan())!;
        return value;
    }

    /// <summary>
    /// Извлекает все первичные ключи по префиксу.
    /// </summary>
    /// <param name="prefixKey">Префиксный ключ. Должен иметь общую префиксную часть с первичным ключом <typeparamref name="TKey"/>.</param>
    /// <param name="isValidPrefix">Предикат, проверяющий что префикс валиден для первичного ключа.</param>
    public IEnumerable<TKey> GetAllKeysByPrefix<TPrefixKey>(TPrefixKey prefixKey, Func<TKey, TPrefixKey, bool> isValidPrefix)
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
    public IEnumerable<TKey> GetAllKeys()
    {
        using var iterator = _rocksDb.NewIterator(_columnFamily.Handle);
        for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next())
        {
            var fullKey = MemoryPackSerializer.Deserialize<TKey>(iterator.GetKeySpan())!;
            yield return fullKey;
        }
    }

    #endregion

    #region Remove

    /// <summary>
    /// Удаляет запись по ключу, если имеется.
    /// Выполняется сразу, без транзакции.
    /// </summary>
    public void Remove(TKey key)
    {
        var transaction = _rocksDb.CreateMockedTransaction();
        try
        {
            RemoveCore(key, ref transaction);
            transaction.Commit();
        }
        finally
        {
            transaction.Dispose();
        }
    }

    /// <summary>
    /// Удаляет запись по ключу, если имеется.
    /// </summary>
    /// <param name="key">Ключ.</param>
    /// <param name="changeTransaction">Транзакция, в рамках которой проводится операция.</param>
    public void Remove(TKey key, ref ChangeTransaction<WriteBatchCommandRocksDbWrapper> changeTransaction)
    {
        RemoveCore(key, ref changeTransaction);
    }

    /// <summary>
    /// Вычисляет на основе <paramref name="value"/> ключ и удаляет по нему запись, если имеется.
    /// Выполняется сразу, без транзакции.
    /// </summary>
    public void Remove(TValue value)
    {
        var key = _keyProvider(value);
        Remove(key);
    }

    /// <summary>
    /// Вычисляет на основе <paramref name="value"/> ключ и удаляет по нему запись, если имеется.
    /// </summary>
    /// <param name="value">Значение.</param>
    /// <param name="changeTransaction">Транзакция, в рамках которой проводится операция.</param>
    public void Remove(TValue value, ref ChangeTransaction<WriteBatchCommandRocksDbWrapper> changeTransaction)
    {
        var key = _keyProvider(value);
        RemoveCore(key, ref changeTransaction);
    }

    private void RemoveCore<TWrapper>(TKey key, ref ChangeTransaction<TWrapper> changeTransaction)
        where TWrapper : IRocksDbCommandWrapper
    {
        using var keyBuffer = new ArrayPoolBufferWriter();
        MemoryPackSerializer.Serialize(in keyBuffer, in key);

        TValue? oldValue = default;
        if (_changeConsumer is not null)
        {
            oldValue = GetByKey(keyBuffer.WrittenSpan);
        }

        changeTransaction.CommandWrapper.Delete(keyBuffer.WrittenSpan, _columnFamily.Handle);
        RegisterChange(key, oldValue, default, ref changeTransaction);
    }

    #endregion

    #region Put

    /// <summary>
    /// Помещает запись в хранилище. Если запись существовало ранее, перезаписывает её.
    /// Выполняется сразу, без транзакции.
    /// </summary>
    public void Set(TValue newValue)
    {
        ActualizeIfChanged(newValue, default);
    }

    /// <summary>
    /// Помещает запись в хранилище. Если запись существовало ранее, перезаписывает её.
    /// </summary>
    /// <param name="newValue">Значение.</param>
    /// <param name="changeTransaction">Транзакция, в рамках которой проводится операция.</param>
    public void Set(TValue newValue, ref ChangeTransaction<WriteBatchCommandRocksDbWrapper> changeTransaction)
    {
        ActualizeIfChanged(newValue, default, ref changeTransaction);
    }

    /// <summary>
    /// Актуализирует значение.
    /// Если передан <paramref name="comparisonOldValue"/> и относительно него поменялся первичный ключ, выполняет удаление старой записи со старым первичным ключом.
    /// Затем добавляет новую запись <paramref name="newValue"/>.
    /// Выполняется сразу, без транзакции.
    /// </summary>
    /// <param name="newValue">Новое значение.</param>
    /// <param name="comparisonOldValue">Старое значение для сравнения.</param>
    public void ActualizeIfChanged(TValue newValue, TValue? comparisonOldValue)
    {
        var transaction = _rocksDb.CreateMockedTransaction();
        try
        {
            ActualizeIfChangedCore(newValue, comparisonOldValue, ref transaction);
            transaction.Commit();
        }
        finally
        {
            transaction.Dispose();
        }
    }

    /// <summary>
    /// Актуализирует значение.
    /// Если передан <paramref name="comparisonOldValue"/> и относительно него поменялся первичный ключ, выполняет удаление старой записи со старым первичным ключом.
    /// Затем добавляет новую запись <paramref name="newValue"/>.
    /// </summary>
    /// <param name="newValue">Новое значение.</param>
    /// <param name="comparisonOldValue">Старое значение для сравнения.</param>
    /// <param name="changeTransaction">Транзакция, в рамках которой проводится операция.</param>
    public void ActualizeIfChanged(TValue newValue, TValue? comparisonOldValue, ref ChangeTransaction<WriteBatchCommandRocksDbWrapper> changeTransaction)
    {
        ActualizeIfChangedCore(newValue, comparisonOldValue, ref changeTransaction);
    }

    private void ActualizeIfChangedCore<TWrapper>(TValue newValue, TValue? comparisonOldValue, ref ChangeTransaction<TWrapper> changeTransaction)
        where TWrapper : IRocksDbCommandWrapper
    {
        var newKey = _keyProvider(newValue);
        using var keyBuffer = new ArrayPoolBufferWriter();
        if (comparisonOldValue is not null)
        {
            var oldKey = _keyProvider(comparisonOldValue);
            if (!EqualityComparer<TKey>.Default.Equals(newKey, oldKey))
            {
                MemoryPackSerializer.Serialize(in keyBuffer, oldKey);
                changeTransaction.CommandWrapper.Delete(keyBuffer.WrittenSpan, _columnFamily.Handle);
                keyBuffer.Reset();
            }
        }

        MemoryPackSerializer.Serialize(in keyBuffer, newKey);
        if (comparisonOldValue is null && _changeConsumer is not null)
        {
            comparisonOldValue = GetByKey(keyBuffer.WrittenSpan);
        }

        changeTransaction.CommandWrapper.Put(keyBuffer.WrittenSpan, changeTransaction.GetSerializedValue(newValue), _columnFamily.Handle);
        RegisterChange(newKey, comparisonOldValue, newValue, ref changeTransaction);
    }

    #endregion

    private void RegisterChange<TWrapper>(TKey key, TValue? oldValue, TValue? newValue, ref ChangeTransaction<TWrapper> changeTransaction)
        where TWrapper : IRocksDbCommandWrapper
    {
        if (_changeConsumer is null)
        {
            return;
        }

        changeTransaction.RegisterChange(new Change<TKey, TValue>(key, oldValue, newValue, _changeConsumer));
    }
}