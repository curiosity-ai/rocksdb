using System.Text;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.CompatTests.Audit4;

/// <summary>Audit #4, slice TableReader: differential checks against native librocksdb.</summary>
[TestClass]
public sealed class Audit4TableReaderCompatTests
{
    private static readonly System.Collections.Concurrent.ConcurrentBag<object> s_keepAlive = new();

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static TableProperties ReadProperties(string path)
    {
        using var reader = new SstFileReader(new ColumnFamilyOptions());
        reader.Open(path);
        return reader.GetTableProperties();
    }

    /// <summary>
    /// SstFileWriter::Open (table/sst_file_writer.cc): the identity and time properties of an external file written by
    /// the managed SstFileWriter are those native writes: db_id "SST Writer", creation / oldest key / file creation
    /// time 0, the fake file numbers 1, 2, ... of one writer and one session id for its lifetime.
    /// </summary>
    [TestMethod]
    public void SstFileWriterIdentityPropertiesMatchNative()
    {
        string dir = Path.Combine(Path.GetTempPath(), "audit4-tr-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var native = new TableProperties[2];
            var cfo = new RocksDbSharp.ColumnFamilyOptions().SetCompression(RocksDbSharp.Compression.No);
            s_keepAlive.Add(cfo);
            using (var w = new RocksDbSharp.SstFileWriter(new RocksDbSharp.EnvOptions(), cfo))
            {
                for (int i = 0; i < 2; i++)
                {
                    string path = Path.Combine(dir, "native" + i + ".sst");
                    w.Open(path);
                    w.Put(B("a"), B("1"));
                    w.Finish();
                    native[i] = ReadProperties(path);
                }
            }
            var managed = new TableProperties[2];
            using (var w = new SstFileWriter(new ColumnFamilyOptions()))
            {
                for (int i = 0; i < 2; i++)
                {
                    string path = Path.Combine(dir, "managed" + i + ".sst");
                    w.Open(path);
                    w.Put("a"u8, "1"u8);
                    w.Finish();
                    managed[i] = ReadProperties(path);
                }
            }
            for (int i = 0; i < 2; i++)
            {
                Assert.AreEqual(native[i].DbId, managed[i].DbId);
                Assert.AreEqual(native[i].CreationTime, managed[i].CreationTime);
                Assert.AreEqual(native[i].OldestKeyTime, managed[i].OldestKeyTime);
                Assert.AreEqual(native[i].FileCreationTime, managed[i].FileCreationTime);
                Assert.AreEqual(native[i].OriginalFileNumber, managed[i].OriginalFileNumber);
                Assert.AreEqual(native[i].DbSessionId.Length, managed[i].DbSessionId.Length);
            }
            Assert.AreEqual(native[0].DbSessionId == native[1].DbSessionId, managed[0].DbSessionId == managed[1].DbSessionId);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
