using System.Buffers;
using RocksDbTable.Serialization;

namespace Tests.Infrastructure;

public class StudentRockSerializer : IRockSerializer<Student>
{
    public static readonly StudentRockSerializer Instance = new();

    public void Serialize(IBufferWriter<byte> writer, Student value)
    {
        Int32RockSerializer.Instance.Serialize(writer, value.Id);
        Int32RockSerializer.Instance.Serialize(writer, value.Name.Length);
        StringRockSerializer.Utf8.Serialize(writer, value.Name);
        StringRockSerializer.Utf8.Serialize(writer, value.PassportId);
    }

    public Student Deserialize(ReadOnlySpan<byte> span)
    {
        var id = Int32RockSerializer.Instance.Deserialize(span.Slice(0, 4));
        var nameLength = Int32RockSerializer.Instance.Deserialize(span.Slice(4, 4));
        var name = StringRockSerializer.Utf8.Deserialize(span.Slice(8, nameLength));
        var passportId = StringRockSerializer.Utf8.Deserialize(span.Slice(8 + nameLength));
        return new Student(id, name, passportId);
    }
}