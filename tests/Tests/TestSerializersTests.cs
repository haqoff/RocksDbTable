using System.Buffers;
using Tests.Infrastructure;

namespace Tests;

public class TestSerializersTests
{
    [Fact]
    public void Student()
    {
        var data = new Student(55, "John Doe", "7123 1239 9129");

        var writer = new ArrayBufferWriter<byte>();
        StudentRockSerializer.Instance.Serialize(writer, data);
        var serializedBytes = writer.WrittenSpan.ToArray();

        var deserializedData = StudentRockSerializer.Instance.Deserialize(serializedBytes);
        Assert.Equal(data, deserializedData);
    }

    [Fact]
    public void CounterData()
    {
        var data = new CounterData("Name", 55);

        var writer = new ArrayBufferWriter<byte>();
        CounterDataSerializer.Instance.Serialize(writer, data);
        var serializedBytes = writer.WrittenSpan.ToArray();

        var deserializedData = CounterDataSerializer.Instance.Deserialize(serializedBytes);
        Assert.Equal(data, deserializedData);
    }
}