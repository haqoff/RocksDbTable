using System.Text;
using FluentAssertions;
using RocksDbTable.ChangeTracking;
using RocksDbTable.Core;
using RocksDbTable.Extensions;
using RocksDbTable.Options;
using RocksDbTable.Serialization;
using RocksDbTable.Tables;
using Moq;
using RocksDbSharp;
using Tests.Infrastructure;

namespace Tests;

public class RocksDbTableTests : IDisposable
{
    private readonly string _rocksDbPath;
    private readonly RocksDb _rocksDb;

    public RocksDbTableTests()
    {
        _rocksDb = TestHelper.CreateTempRocksDb(out _rocksDbPath);
    }

    #region Put

    [Fact]
    public void Put_NoTransactionSpecified_NoIndexes_NoChangesConsumer_ShouldAddValue()
    {
        // Arrange
        var putValue = new Student(100, "John Doe", "1729 1239 1239 9999");
        var table = CreateTable();

        // Act
        table.Put(putValue);

        // Assert
        var value = table.GetAllValues().Single();
        Assert.Equal(putValue, value);
    }

    [Fact]
    public void Put_TransactionSpecified_NoIndexes_NoChangesConsumer_ShouldAddValue()
    {
        // Arrange
        var transaction = _rocksDb.CreateTransaction();
        var putValue = new Student(100, "John Doe", "1729 1239 1239 9999");
        var table = CreateTable();

        // Act, Assert
        table.Put(putValue, ref transaction);
        table.GetAllValues().Should().BeEmpty();

        transaction.Commit();

        var value = table.GetAllValues().Single();
        Assert.Equal(putValue, value);
    }

    [Fact]
    public void Put_NoTransactionSpecified_WithIndexes_NoChangesConsumer_ShouldAddMultipleValues()
    {
        // Arrange
        var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
        var putValue2 = new Student(200, "John Doe", "1111 2222 3333 4444");
        var putValue3 = new Student(300, "John Snow", "4543 3241 4531 6124");

        var table = CreateTable();
        var nameNotUniqueIndex = table.CreateNotUniqueIndex(s => s.Name, StringRockSerializer.Utf8);
        var passportIdUniqueIndex = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8);

        // Act
        table.Put(putValue1);
        table.Put(putValue2);
        table.Put(putValue3);

        // Assert
        table.GetAllValues().Should().BeEquivalentTo([putValue1, putValue2, putValue3]);
        nameNotUniqueIndex.GetAllValuesByKey("John Doe").Should().BeEquivalentTo([putValue1, putValue2]);
        passportIdUniqueIndex.GetByKey("1111 2222 3333 4444").Should().Be(putValue2);
    }

    [Fact]
    public void Put_NoTransactionSpecified_WithIndexes_WithChangesConsumer_ShouldAddValueAndDispatchAddEvent()
    {
        // Arrange
        var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
        var tableChangesConsumerMock = new Mock<IRocksDbTableChangesConsumer<int, Student>>();

        var table = CreateTable(opt => opt.SetTableChangesConsumer(tableChangesConsumerMock.Object));
        _ = table.CreateNotUniqueIndex(s => s.Name, StringRockSerializer.Utf8);
        _ = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8);

        // Act
        table.Put(putValue1);

        // Assert
        tableChangesConsumerMock.Verify(x => x.AddedOrUpdated(It.Is<int>(i => i == 100), null, putValue1), Times.Once);
        table.GetAllValues().Should().BeEquivalentTo([putValue1]);
    }

    [Fact]
    public async Task Put_NoTransactionSpecified_WithIndexes_NoChangesConsumer_WithConcurrentChanges_DataShouldBeConsistent()
    {
        // Arrange
        var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
        var putValue2 = new Student(100, "John Snow", "1728 1234 1231 9991");

        var table = CreateTable(opt => opt.SetEnableConcurrentChangesWithinRow(true));
        var nameNotUniqueIndex = table.CreateNotUniqueIndex(s => s.Name, StringRockSerializer.Utf8);
        var passportIdUniqueIndex = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8);

        var putTask1 = Task.Run(() =>
        {
            for (int i = 0; i < 10000; i++)
            {
                table.Put(putValue1);
            }
        });

        var putTask2 = Task.Run(() =>
        {
            for (int i = 0; i < 10000; i++)
            {
                table.Put(putValue2);
            }
        });

        // Act
        await Task.WhenAll(putTask1, putTask2);

        // Assert
        var actualValue = table.GetAllValues().Single();
        nameNotUniqueIndex.GetFirstValue("John").Should().Be(actualValue);
        passportIdUniqueIndex.GetAllValuesByPrefix("172", StringRockSerializer.Utf8).Should().BeEquivalentTo([actualValue]);
    }

    [Fact]
    public void Put_WithIndexes_UpdateShouldBePerformedCorrectly()
    {
        // Arrange
        var beforePutValue = new Student(100, "John Doe", "1729 1239 1239 9999");
        var afterPutValue = new Student(100, "John Snow", "1728 1234 1231 9991");
        var anotherPutValue = new Student(101, "John Throw", "1111 2222 1231 9991");

        var table = CreateTable(opt => opt.SetEnableConcurrentChangesWithinRow(true));
        var nameIndex = table.CreateNotUniqueIndex(s => s.Name, StringRockSerializer.Utf8);
        var passportIdIndex = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8);

        // Act, Assert
        table.Put(anotherPutValue);

        table.Put(beforePutValue);
        AssertCurrentValue(beforePutValue);

        table.Put(afterPutValue);
        AssertCurrentValue(afterPutValue);

        // check old index data deleted
        nameIndex.GetAllValuesByKey(beforePutValue.Name).Should().BeEmpty();
        passportIdIndex.GetByKey(beforePutValue.PassportId).Should().BeNull();


        // AssertCurrentValue
        void AssertCurrentValue(Student expected)
        {
            table.GetByKey(100).Should().Be(expected);
            nameIndex.GetAllValuesByKey(expected.Name).Should().BeEquivalentTo([expected]);
            passportIdIndex.GetByKey(expected.PassportId).Should().Be(expected);
        }
    }

    [Fact]
    public void Put_NoTransactionSpecified_NoIndexes_WithChangesConsumer_ShouldUpdateValueAndDispatchUpdateEvent()
    {
        // Arrange
        var beforePutValue = new Student(100, "John Doe", "1729 1239 1239 9999");
        var afterPutValue = new Student(100, "John Snow", "1728 1234 1231 9991");
        var tableChangesConsumerMock = new Mock<IRocksDbTableChangesConsumer<int, Student>>();

        var table = CreateTable(opt => opt.SetTableChangesConsumer(tableChangesConsumerMock.Object));

        // Act
        table.Put(beforePutValue);
        table.Put(afterPutValue);

        // Assert
        tableChangesConsumerMock.Verify(x => x.AddedOrUpdated(It.Is<int>(i => i == 100), null, beforePutValue), Times.Once);
        tableChangesConsumerMock.Verify(x => x.AddedOrUpdated(It.Is<int>(i => i == 100), beforePutValue, afterPutValue), Times.Once);
    }

    [Fact]
    public void Put_WithReferenceIndexes_ShouldAddAndGetCorrectly()
    {
        // Arrange
        var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
        var putValue2 = new Student(1001, "John Doe", "1728 1234 1231 9991");

        var table = CreateTable();
        var nameNotUniqueIndex = table.CreateNotUniqueIndex(s => s.Name, StringRockSerializer.Utf8, opt => opt.SetValueStoreMode(ValueStoreMode.Reference));
        var passportIdUniqueIndex = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8, opt => opt.SetValueStoreMode(ValueStoreMode.Reference));

        // Act
        table.Put(putValue1);
        table.Put(putValue2);

        // Assert
        table.GetAllValues().Should().BeEquivalentTo([putValue1, putValue2]);
        nameNotUniqueIndex.GetAllValuesByKey("John Doe").Should().BeEquivalentTo([putValue1, putValue2]);
        passportIdUniqueIndex.GetByKey(putValue1.PassportId).Should().Be(putValue1);
        passportIdUniqueIndex.GetByKey(putValue2.PassportId).Should().Be(putValue2);

        // assert that unique index stores only reference
        {
            var storedValueBytes = ((KeyValueStoreBase<string, Student>)passportIdUniqueIndex).GetRaw(Encoding.UTF8.GetBytes(putValue1.PassportId))!;
            var reference = BitConverter.ToInt32(storedValueBytes);
            reference.Should().Be(putValue1.Id);
        }

        // assert that not unique index stores only reference
        {
            var s = new MemoryStream();
            s.Write(Encoding.UTF8.GetBytes(putValue1.Name));
            s.Write(BitConverter.GetBytes(putValue1.Id));
            var storedValueBytes = ((KeyValueStoreBase<string, Student>)nameNotUniqueIndex).GetRaw(s.ToArray())!;
            var reference = BitConverter.ToInt32(storedValueBytes);
            reference.Should().Be(putValue1.Id);
        }
    }

    [Fact]
    public void Put_WithReferenceIndexes_ShouldUpdateAndGetCorrectly()
    {
        // Arrange
        var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
        var putValue2 = new Student(100, "John Snow", "1728 1234 1231 9991");

        var table = CreateTable();
        var nameNotUniqueIndex = table.CreateNotUniqueIndex(s => s.Name, StringRockSerializer.Utf8, opt => opt.SetValueStoreMode(ValueStoreMode.Reference));
        var passportIdUniqueIndex = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8, opt => opt.SetValueStoreMode(ValueStoreMode.Reference));

        table.Put(putValue1);
        table.GetAllValues().Should().BeEquivalentTo([putValue1]);

        // Act
        table.Put(putValue2);

        // Assert
        table.GetAllValues().Should().BeEquivalentTo([putValue2]);

        nameNotUniqueIndex.GetAllValuesByKey(putValue2.Name).Should().BeEquivalentTo([putValue2]);
        nameNotUniqueIndex.GetAllValuesByKey(putValue1.Name).Should().BeEmpty();

        passportIdUniqueIndex.GetByKey(putValue1.PassportId).Should().BeNull();
        passportIdUniqueIndex.GetByKey(putValue2.PassportId).Should().Be(putValue2);
    }

    [Fact]
    public void Put_WithReferenceUniqueIndex_ShouldUpdateTableRowButNotUpdateIndexReferences()
    {
        // Arrange
        var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
        var putValue2 = new Student(100, "John Snow", "1729 1239 1239 9999");

        var table = CreateTable();
        var passportIdUniqueIndex = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8, opt => opt.SetValueStoreMode(ValueStoreMode.Reference));

        table.Put(putValue1);
        table.GetAllValues().Should().BeEquivalentTo([putValue1]);
        passportIdUniqueIndex.GetByKey(putValue2.PassportId).Should().Be(putValue1);

        // Act
        table.Put(putValue2);

        // Assert
        table.GetAllValues().Should().BeEquivalentTo([putValue2]);
        passportIdUniqueIndex.GetByKey(putValue2.PassportId).Should().Be(putValue2);
    }

    [Fact]
    public void Put_WithReferenceNotUniqueIndex_ShouldUpdateTableRowButNotUpdateIndexReferences()
    {
        // Arrange
        var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
        var putValue2 = new Student(100, "John Doe", "3333 3333 3333 3333");

        var table = CreateTable();
        var nameIndex = table.CreateNotUniqueIndex(s => s.Name, StringRockSerializer.Utf8, opt => opt.SetValueStoreMode(ValueStoreMode.Reference));

        table.Put(putValue1);
        table.GetAllValues().Should().BeEquivalentTo([putValue1]);
        nameIndex.GetAllValuesByKey(putValue1.Name).Should().BeEquivalentTo([putValue1]);

        // Act
        table.Put(putValue2);

        // Assert
        table.GetAllValues().Should().BeEquivalentTo([putValue2]);
        nameIndex.GetAllValuesByKey(putValue1.Name).Should().BeEquivalentTo([putValue2]);
    }

    [Fact]
    public void Put_TransactionSpecified_WithIndexes_NoChangesConsumer_WithConcurrentSupport_WhenPutSameRowTwiceInsideTransaction_LockHandledCorrectly()
    {
        // Arrange
        var putValue = new Student(100, "John Doe", "1729 1239 1239 9999");
        var table = CreateTable(opt => opt.SetEnableConcurrentChangesWithinRow(true));
        _ = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8);

        // Act, Assert
        var transaction = _rocksDb.CreateTransaction();
        table.Put(putValue, ref transaction);
        table.Put(putValue, ref transaction);
        transaction.TakenLockCount.Should().Be(1);
        transaction.Commit();
        transaction.TakenLockCount.Should().Be(0);

        var value = table.GetAllValues().Single();
        Assert.Equal(putValue, value);
    }

    [Fact]
    public void Put_TransactionSpecified_WithIndexes_NoChangesConsumer_WithConcurrentSupport_2TransactionsShouldHaveCorrectLockCount()
    {
        // Arrange
        var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
        var putValue2 = new Student(100, "John Snow", "1111 2222 3333 4444");
        var table = CreateTable(opt => opt.SetEnableConcurrentChangesWithinRow(true));
        _ = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8);

        // Act, Assert
        var transaction1 = _rocksDb.CreateTransaction();
        table.Put(putValue1, ref transaction1);
        transaction1.TakenLockCount.Should().Be(1);

        var transaction2 = _rocksDb.CreateTransaction();
        table.Put(putValue2, ref transaction2);
        transaction2.TakenLockCount.Should().Be(1);
        transaction2.Commit();

        transaction1.Commit();
        var value = table.GetAllValues().Single();
        Assert.Equal(putValue1, value);
    }

    [Fact]
    public void Put_TransactionSpecified_WithIndexes_NoChangesConsumer_WithConcurrentSupport_2ConcurrentSubsequentTransactions()
    {
        // Arrange
        var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
        var putValue2 = new Student(100, "John Snow", "1111 2222 3333 4444");
        var table = CreateTable(opt => opt.SetEnableConcurrentChangesWithinRow(true));
        _ = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8);

        // Act, Assert
        var transaction1 = _rocksDb.CreateTransaction();
        table.Put(putValue1, ref transaction1);

        var thread = new Thread(() =>
        {
            var transaction2 = _rocksDb.CreateTransaction();
            table.Put(putValue2, ref transaction2);
            transaction2.Commit();
        });

        thread.Start();
        transaction1.Commit();
        thread.Join();

        var value = table.GetAllValues().Single();
        Assert.Equal(putValue2, value);
    }

    #endregion

    #region Remove

    [Fact]
    public void Remove_NoTransactionSpecified_NoIndexes_NoChangesConsumer_ShouldRemoveValue()
    {
        // Arrange
        var putValue = new Student(100, "John Doe", "1729 1239 1239 9999");
        var table = CreateTable();
        table.Put(putValue);

        // Act
        table.Remove(putValue.Id);

        // Assert
        table.GetAllKeys().Should().BeEmpty();
    }

    [Fact]
    public void Remove_WithTransactionSpecified_NoIndexes_NoChangesConsumer_ShouldRemoveValue()
    {
        // Arrange
        var putValue = new Student(100, "John Doe", "1729 1239 1239 9999");
        var table = CreateTable();
        table.Put(putValue);

        // Act, Assert
        var transaction = _rocksDb.CreateTransaction();
        table.Remove(putValue.Id, ref transaction);
        table.GetAllKeys().Should().BeEquivalentTo([putValue.Id]);

        transaction.Commit();
        table.GetAllValues().Should().BeEmpty();
    }

    [Fact]
    public void Remove_NoTransactionSpecified_WithIndexes_NoChangesConsumer_ShouldRemoveValue()
    {
        // Arrange
        var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
        var putValue2 = new Student(300, "John Doe", "4543 3241 4531 6124");

        var table = CreateTable();
        var nameNotUniqueIndex = table.CreateNotUniqueIndex(s => s.Name, StringRockSerializer.Utf8);
        var passportIdUniqueIndex = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8);
        table.Put(putValue1);
        table.Put(putValue2);

        // Act
        table.Remove(putValue1.Id);

        // Assert
        table.GetAllValues().Should().BeEquivalentTo([putValue2]);
        nameNotUniqueIndex.GetAllValuesByKey("John Doe").Should().BeEquivalentTo([putValue2]);
        passportIdUniqueIndex.GetByKey(putValue1.PassportId).Should().BeNull();
    }

    [Fact]
    public void Remove_NoTransactionSpecified_WithIndexes_WithChangesConsumer_ShouldRemoveValueAndDispatchRemovedEvent()
    {
        // Arrange
        var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
        var tableChangesConsumerMock = new Mock<IRocksDbTableChangesConsumer<int, Student>>();

        var table = CreateTable(opt => opt.SetTableChangesConsumer(tableChangesConsumerMock.Object));
        _ = table.CreateNotUniqueIndex(s => s.Name, StringRockSerializer.Utf8);
        _ = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8);
        table.Put(putValue1);
        table.GetByKey(putValue1.Id).Should().NotBeNull();

        // Act
        table.Remove(putValue1.Id);

        // Assert
        tableChangesConsumerMock.Verify(x => x.Removed(It.Is<int>(i => i == 100), putValue1), Times.Once);
        table.GetAllValues().Should().BeEmpty();
    }

    #endregion

    [Fact]
    public async Task PutAndRemove_NoTransactionSpecified_WithIndexes_NoChangesConsumer_WithConcurrentChanges_DataShouldBeConsistent()
    {
        for (var i = 0; i < 100; i++)
        {
            // Arrange
            var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
            var table = CreateTable(opt => opt.SetEnableConcurrentChangesWithinRow(true));
            var nameNotUniqueIndex = table.CreateNotUniqueIndex(s => s.Name, StringRockSerializer.Utf8);
            var passportIdUniqueIndex = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8);

            var putTask1 = Task.Run(() =>
            {
                for (var j = 0; j < 10000; j++)
                {
                    table.Put(putValue1);
                }
            });

            var removeTask = Task.Run(() =>
            {
                for (var j = 0; j < 10000; j++)
                {
                    table.Remove(putValue1.Id);
                }
            });

            // Act
            await Task.WhenAll(putTask1, removeTask);

            // Assert
            var actualValue = table.GetAllValues().FirstOrDefault();
            nameNotUniqueIndex.GetAllValuesByKey(putValue1.Name).FirstOrDefault().Should().Be(actualValue);
            passportIdUniqueIndex.GetByKey(putValue1.PassportId).Should().Be(actualValue);
        }
    }
    
    [Fact]
    public void PutAndRemove_TransactionSpecified_WithIndexes_NoChangesConsumer_WithConcurrentSupport_2ConcurrentSubsequentTransactions()
    {
        // Arrange
        var putValue1 = new Student(100, "John Doe", "1729 1239 1239 9999");
        var table = CreateTable(opt => opt.SetEnableConcurrentChangesWithinRow(true));
        _ = table.CreateUniqueIndex(s => s.PassportId, StringRockSerializer.Utf8);

        // Act, Assert
        var transaction1 = _rocksDb.CreateTransaction();
        table.Put(putValue1, ref transaction1);

        var thread = new Thread(() =>
        {
            var transaction2 = _rocksDb.CreateTransaction();
            table.Remove(putValue1.Id, ref transaction2);
            transaction2.Commit();
        });

        thread.Start();
        transaction1.Commit();
        thread.Join();

        table.GetAllValues().Should().BeEmpty();
    }

    public void Dispose()
    {
        _rocksDb.Dispose();
        Directory.Delete(_rocksDbPath, true);
    }

    private IRocksDbTable<int, Student> CreateTable(Action<TableOptions<int, Student>>? action = null)
    {
        return _rocksDb.CreateTable(s => s.Id, Int32RockSerializer.Instance, StudentSerializer.Instance, action);
    }
}