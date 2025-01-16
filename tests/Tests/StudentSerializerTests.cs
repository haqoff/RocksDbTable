using System.Buffers;
using Tests.Infrastructure;

namespace Tests;

public class StudentSerializerTests
{
    [Fact]
    public void Test()
    {
        var data = new Student(55, "John Doe", "7123 1239 9129");

        var writer = new ArrayBufferWriter<byte>();
        StudentSerializer.Instance.Serialize(writer, data);
        var serializedBytes = writer.WrittenSpan.ToArray();

        var deserializedData = StudentSerializer.Instance.Deserialize(serializedBytes);
        Assert.Equal(data, deserializedData);
    }
}