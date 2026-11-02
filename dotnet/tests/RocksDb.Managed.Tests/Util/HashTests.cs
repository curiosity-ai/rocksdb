using System.Text;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tests.Util;

/// <summary>Port of util/hash_test.cc (value schema tests: these hashes are part of persisted formats).</summary>
[TestClass]
public sealed class HashTests
{
    private static byte[] B(params int[] bytes)
    {
        byte[] r = new byte[bytes.Length];
        for (int i = 0; i < bytes.Length; i++) r[i] = (byte)bytes[i];
        return r;
    }

    private static readonly (byte[] Input, uint Hash32, ulong Hash64)[] SmallVectors =
    [
        (B(), 3164544308u, 5999572062939766020UL),
        (B(0x08), 422599524u, 583283813901344696UL),
        (B(0x17), 3168152998u, 16175549975585474943UL),
        (B(0x9a), 3195034349u, 16322991629225003903UL),
        (B(0x1c), 2651681383u, 13269285487706833447UL),
        (B(0x4d, 0x76), 2447836956u, 6859542833406258115UL),
        (B(0x52, 0xd5), 3854228105u, 4919611532550636959UL),
        (B(0x91, 0xf7), 31066776u, 14199427467559720719UL),
        (B(0xd6, 0x27), 1806091603u, 12292689282614532691UL),
        (B(0x30, 0x46, 0x0b), 3808221797u, 11404699285340020889UL),
        (B(0x56, 0xdc, 0xd6), 2157698265u, 12404347133785524237UL),
        (B(0xd4, 0x52, 0x33), 1721992661u, 15853805298481534034UL),
        (B(0x6a, 0xb5, 0xf4), 2469105222u, 16863488758399383382UL),
        (B(0x67, 0x53, 0x81, 0x1c), 118283265u, 9010661983527562386UL),
        (B(0x69, 0xb8, 0xc0, 0x88), 3416318611u, 6611781377647041447UL),
        (B(0x1e, 0x84, 0xaf, 0x2d), 3315003572u, 15290969111616346501UL),
        (B(0x46, 0xdc, 0x54, 0xbe), 447346355u, 7063754590279313623UL),
        (B(0xd0, 0x7a, 0x6e, 0xea, 0x56), 4255445370u, 6384167718754869899UL),
        (B(0x86, 0x83, 0xd5, 0xa4, 0xd8), 2390603402u, 16874407254108011067UL),
        (B(0xb7, 0x46, 0xbb, 0x77, 0xce), 2048907743u, 16809880630149135206UL),
        (B(0x6c, 0xa8, 0xbc, 0xe5, 0x99), 2177978500u, 1249038833153141148UL),
        (B(0x5c, 0x5e, 0xe1, 0xa0, 0x73, 0x81), 1036846008u, 17358142495308219330UL),
        (B(0x08, 0x5d, 0x73, 0x1c, 0xe5, 0x2e), 229980482u, 4237646583134806322UL),
        (B(0x42, 0xfb, 0xf2, 0x52, 0xb4, 0x10), 3655585422u, 4373664924115234051UL),
        (B(0x73, 0xe1, 0xff, 0x56, 0x9c, 0xce), 3502708029u, 12012981210634596029UL),
        (B(0x5c, 0xbe, 0x97, 0x75, 0x54, 0x9a, 0x52), 815120748u, 5716522398211028826UL),
        (B(0x16, 0x82, 0x39, 0x49, 0x88, 0x2b, 0x36), 3056033698u, 15604531309862565013UL),
        (B(0x59, 0x77, 0xf0, 0xa7, 0x24, 0xf4, 0x78), 587205227u, 8601330687345614172UL),
        (B(0xd3, 0xa5, 0x7c, 0x0e, 0xc0, 0x02, 0x07), 2030937252u, 8088079329364056942UL),
        (B(0x31, 0x1b, 0x98, 0x75, 0x96, 0x22, 0xd3, 0x9a), 469635402u, 9844314944338447628UL),
        (B(0x38, 0xd6, 0xf7, 0x28, 0x20, 0xb4, 0x8a, 0xe9), 3530274698u, 10973293517982163143UL),
        (B(0xbb, 0x18, 0x5d, 0xf4, 0x12, 0x03, 0xf7, 0x99), 1974545809u, 9986007080564743219UL),
        (B(0x80, 0xd4, 0x3b, 0x3b, 0xae, 0x22, 0xa2, 0x78), 3563570120u, 1729303145008254458UL),
        (B(0x1a, 0xb5, 0xd0, 0xfe, 0xab, 0xc3, 0x61, 0xb2, 0x99), 2706087434u, 13253403748084181481UL),
        (B(0x8e, 0x4a, 0xc3, 0x18, 0x20, 0x2f, 0x06, 0xe6, 0x3c), 1534654151u, 7768754303876232188UL),
        (B(0xb6, 0xc0, 0xdd, 0x05, 0x3f, 0xc4, 0x86, 0x4c, 0xef), 2355554696u, 12439346786701492UL),
        (B(0x9a, 0x5f, 0x78, 0x0d, 0xaf, 0x50, 0xe1, 0x1f, 0x55), 1400800912u, 10841838338450144690UL),
        (B(0x22, 0x6f, 0x39, 0x1f, 0xf8, 0xdd, 0x4f, 0x52, 0x17, 0x94), 3420325137u, 12883919702069153152UL),
        (B(0x32, 0x89, 0x2a, 0x75, 0x48, 0x3a, 0x4a, 0x02, 0x69, 0xdd), 3427803584u, 12692903507676842188UL),
        (B(0x06, 0x92, 0x5c, 0xf4, 0x88, 0x0e, 0x7e, 0x68, 0x38, 0x3e), 1152407945u, 6540985900674032620UL),
        (B(0xbd, 0x2c, 0x63, 0x38, 0xbf, 0xe9, 0x78, 0xb7, 0xbf, 0x15), 3382479516u, 10551812464348219044UL),
    ];

    [TestMethod]
    public void Hash32Values()
    {
        foreach (var (input, h32, _) in SmallVectors)
        {
            Assert.AreEqual(h32, Hash.Hash32(input, Hash.BloomHashSeed), Convert.ToHexString(input));
            Assert.AreEqual(h32, Hash.BloomHash(input));
        }
    }

    [TestMethod]
    public void Hash64SmallValueSchema()
    {
        foreach (var (input, _, h64) in SmallVectors)
        {
            Assert.AreEqual(h64, Hash.Hash64(input, 0), Convert.ToHexString(input));
            Assert.AreEqual(h64, Hash.GetSliceHash64(input));
        }
    }

    private const string Mod61 = "abcdefghijklmnopqrstuvwxyz123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private static byte[] Repeat(string repeat, int limit)
    {
        var sb = new StringBuilder();
        while (sb.Length < limit) sb.Append(repeat);
        return Encoding.ASCII.GetBytes(sb.ToString());
    }

    private static string Hash64Descriptor(string repeat, int limit)
    {
        byte[] input = Repeat(repeat, limit);
        var rv = new StringBuilder();
        for (int i = 0; i < limit; ++i)
        {
            ulong h = Hash.GetSliceHash64(input.AsSpan(0, i));
            rv.Append(Mod61[(int)(h % 61)]);
        }
        return rv.ToString();
    }

    private static string Hash128Descriptor(string repeat, int limit)
    {
        byte[] input = Repeat(repeat, limit);
        var rv = new StringBuilder();
        for (int i = 0; i < limit; ++i)
        {
            UInt128 h = Hash.GetSliceHash128(input.AsSpan(0, i));
            ulong h2 = (ulong)(h >> 64) + (ulong)h;
            rv.Append(Mod61[(int)(h2 % 61)]);
        }
        return rv.ToString();
    }

    [TestMethod]
    public void Hash64LargeValueSchema()
    {
        Assert.AreEqual(
            "cRhyWsY67B6klRA1udmOuiYuX7IthyGBKqbeosz2hzVglWCmQx8nEdnpkvPfYX56Up2OWOTV" +
            "lTzfAoYwvtqKzjD8E9xttR2unelbXbIV67NUe6bOO23BxaSFRcA3njGu5cUWfgwOqNoTsszp" +
            "uPvKRP6qaUR5VdoBkJUCFIefd7edlNK5mv6JYWaGdwxehg65hTkTmjZoPKxTZo4PLyzbL9U4" +
            "xt12ITSfeP2MfBHuLI2z2pDlBb44UQKVMx27LEoAHsdLp3WfWfgH3sdRBRCHm33UxCM4QmE2" +
            "xJ7gqSvNwTeH7v9GlC8zWbGroyD3UVNeShMLx29O7tH1biemLULwAHyIw8zdtLMDpEJ8m2ic" +
            "l6Lb4fDuuFNAs1GCVUthjK8CV8SWI8Rsz5THSwn5CGhpqUwSZcFknjwWIl5rNCvDxXJqYr",
            Hash64Descriptor("foo", 430));
        Assert.AreEqual(
            "c1EeRkrzgOYWLA8PuhJrwTePJewoB44WdXYDfhbk3ZxTqqg25WlPExDl7IKIQLJvnA6gJxxn" +
            "9TCSLkFGfJeXehaSS1GBqWSzfhEH4VXiXIUCuxJXxtKXcSC6FrNIQGTZbYDiUOLD6Y5inzrF" +
            "9etwQhXUBanw55xAUdNMFQAm2GjJ6UDWp2mISLiMMkLjANWMKLaZMqaFLX37qB4MRO1ooVRv" +
            "zSvaNRSCLxlggQCasQq8icWjzf3HjBlZtU6pd4rkaUxSzHqmo9oM5MghbU5Rtxg8wEfO7lVN" +
            "5wdMONYecslQTwjZUpO1K3LDf3K3XK6sUXM6ShQQ3RHmMn2acB4YtTZ3QQcHYJSOHn2DuWpa" +
            "Q8RqzX5lab92YmOLaCdOHq1BPsM7SIBzMdLgePNsJ1vvMALxAaoDUHPxoFLO2wx18IXnyX",
            Hash64Descriptor("Rocks", 430));
        Assert.AreEqual(
            "c1EeRkukbkb28wLTahwD2sfUhZzaBEnF8SVrxnPVB6A7b8CaAl3UKsDZISF92GSq2wDCukOq" +
            "Jgrsp7A3KZhDiLW8dFXp8UPqPxMCRlMdZeVeJ2dJxrmA6cyt99zkQFj7ELbut6jAeVqARFnw" +
            "fnWVXOsaLrq7bDCbMcns2DKvTaaqTCLMYxI7nhtLpFN1jR755FRQFcOzrrDbh7QhypjdvlYw" +
            "cdAMSZgp9JMHxbM23wPSuH6BOFgxejz35PScZfhDPvTOxIy1jc3MZsWrMC3P324zNolO7JdW" +
            "CX2I5UDKjjaEJfxbgVgJIXxtQGlmj2xkO5sPpjULQV4X2HlY7FQleJ4QRaJIB4buhCA4vUTF" +
            "eMFlxCIYUpTCsal2qsmnGOWa8WCcefrohMjDj1fjzSvSaQwlpyR1GZHF2uPOoQagiCpHpm",
            Hash64Descriptor("RocksDB", 430));
    }

    [TestMethod]
    public void Hash128ValueSchema()
    {
        Assert.AreEqual(
            "bUMA3As8n9I4vNGhThXlEevxZlyMcbb6TYAlIKJ2f5ponsv99q962rYclQ7u3gfnRdCDQ5JI" +
            "2LrGUaCycbXrvLFe4SjgRb9RQwCfrnmNQ7VSEwSKMnkGCK3bDbXSrnIh5qLXdtvIZklbJpGH" +
            "Dqr93BlqF9ubTnOSYkSdx89XvQqflMIW8bjfQp9BPjQejWOeEQspnN1D3sfgVdFhpaQdHYA5" +
            "pI2XcPlCMFPxvrFuRr7joaDvjNe9IUZaunLPMewuXmC3EL95h52Ju3D7y9RNKhgYxMTrA84B" +
            "yJrMvyjdm3vlBxet4EN7v2GEyjbGuaZW9UL6lrX6PghJDg7ACfLGdxNbH3qXM4zaiG2RKnL5" +
            "S3WXKR78RBB5fRFQ8KDIEQjHFvSNsc3GrAEi6W8P2lv8JMTzjBODO2uN4wadVQFT9wpGfV",
            Hash128Descriptor("foo", 430));
        Assert.AreEqual(
            "b35D2vzvklFVDqJmyLRXyApwGGO3EAT3swhe8XJAN3mY2UVPglzdmydxcba6JI2tSvwO6zSu" +
            "ANpjSM7tc9G5iMhsa7R8GfyCXRO1TnLg7HvdWNdgGGBirxZR68BgT7TQsYJt6zyEyISeXI1n" +
            "MXA48Xo7dWfJeYN6Z4KWlqZY7TgFXGbks9AX4ehZNSGtIhdO5i58qlgVX1bEejeOVaCcjC79" +
            "67DrMfOKds7rUQzjBa77sMPcoPW1vu6ljGJPZH3XkRyDMZ1twxXKkNxN3tE8nR7JHwyqBAxE" +
            "fTcjbOWrLZ1irWxRSombD8sGDEmclgF11IxqEhe3Rt7gyofO3nExGckKkS9KfRqsCHbiUyva" +
            "JGkJwUHRXaZnh58b4i1Ei9aQKZjXlvIVDixoZrjcNaH5XJIJlRZce9Z9t82wYapTpckYSg",
            Hash128Descriptor("Rocks", 430));
        Assert.AreEqual(
            "b35D2vFUst3XDZCRlSrhmYYakmqImV97LbBsV6EZlOEQpUPH1d1sD3xMKAPlA5UErHehg5O7" +
            "n966fZqhAf3hRc24kGCLfNAWjyUa7vSNOx3IcPoTyVRFZeFlcCtfl7t1QJumHOCpS33EBmBF" +
            "hvK13QjBbDWYWeHQhJhgV9Mqbx17TIcvUkEnYZxb8IzWNmjVsJG44Z7v52DjGj1ZzS62S2Vv" +
            "qWcDO7apvH5VHg68E9Wl6nXP21vlmUqEH9GeWRehfWVvY7mUpsAg5drHHQyDSdiMceiUuUxJ" +
            "XJqHFcDdzbbPk7xDvbLgWCKvH8k3MpQNWOmbSSRDdAP6nGlDjoTToYkcqVREHJzztSWAAq5h" +
            "GHSUNJ6OxsMHhf8EhXfHtKyUzRmPtjYyeckQcGmrQfFFLidc6cjMDKCdBG6c6HVBrS7H2R",
            Hash128Descriptor("RocksDB", 430));
    }

    /// <summary>Subset of Hash64Misc / Hash128Misc: seeds and sizes change the hash; seeded variants agree.</summary>
    [TestMethod]
    public void SeedAndSizeSensitivity()
    {
        foreach (byte fill in new byte[] { 0, (byte)'a', (byte)'1', 0xff })
        {
            byte[] str = new byte[300];
            Array.Fill(str, fill);
            for (int size = 0; size <= 300; size += 7)
            {
                ReadOnlySpan<byte> s = str.AsSpan(0, size);
                ulong here = Hash.Hash64(s, 0);
                Assert.AreEqual(here, Hash.Hash64(s));
                Assert.AreEqual(here, ((ulong)Hash.Upper32of64(here) << 32) | Hash.Lower32of64(here));
                for (ulong seed = 1; seed != 0; seed <<= 1)
                {
                    Assert.AreNotEqual(here, Hash.Hash64(s, seed));
                }
                for (int smaller = 1; smaller <= Math.Min(30, size); ++smaller)
                {
                    Assert.AreNotEqual(here, Hash.Hash64(str.AsSpan(0, size - smaller), 0));
                }
                UInt128 h128 = Hash.Hash128(s, 0);
                Hash.Hash2x64(s, out ulong hi, out ulong lo);
                Assert.AreEqual((ulong)(h128 >> 64), hi);
                Assert.AreEqual((ulong)h128, lo);
            }
        }
    }

    [TestMethod]
    public void BijectiveHashMatchesHash128For16Bytes()
    {
        var rnd = new Random(1234);
        byte[] buf = new byte[16];
        foreach (ulong seed in new ulong[] { 0, 1, 0x8000000000000000UL, 12345678901234UL })
        {
            for (int i = 0; i < 200; i++)
            {
                rnd.NextBytes(buf);
                ulong inLo = Coding.DecodeFixed64(buf);
                ulong inHi = Coding.DecodeFixed64(buf.AsSpan(8));
                Hash.Hash2x64(buf, seed, out ulong eHi, out ulong eLo);
                Hash.BijectiveHash2x64(inHi, inLo, seed, out ulong hi, out ulong lo);
                Assert.AreEqual(eHi, hi);
                Assert.AreEqual(eLo, lo);
                Hash.BijectiveUnhash2x64(hi, lo, seed, out ulong uHi, out ulong uLo);
                Assert.AreEqual(inHi, uHi);
                Assert.AreEqual(inLo, uLo);
            }
        }
    }

    [TestMethod]
    public void Hash64TrivialNonZero()
    {
        for (ulong seed = 0; seed < 0x1000; ++seed)
        {
            Assert.AreNotEqual(0UL, Hash.Hash64([], seed));
        }
        byte[] input = new byte[4];
        for (int len = 1; len <= 2; ++len)
        {
            for (uint i = 0; (i >> (len * 8)) == 0; ++i)
            {
                Coding.EncodeFixed32(input, i);
                Assert.AreNotEqual(0UL, Hash.Hash64(input.AsSpan(0, len), 0));
            }
        }
    }

    [TestMethod]
    public void FastRangeValues()
    {
        // From FastRange32Test / FastRange64Test in hash_test.cc
        Assert.AreEqual(0u, Hash.FastRange32(0x00000000, 4));
        Assert.AreEqual(1u, Hash.FastRange32(0x40000000, 4));
        Assert.AreEqual(3u, Hash.FastRange32(0xffffffff, 4));
        Assert.AreEqual(0x7fffffffu, Hash.FastRange32(0xffffffff, 0x80000000));
        Assert.AreEqual(1UL, Hash.FastRange64(0x4000000000000000UL, 4));
        Assert.AreEqual(3UL, Hash.FastRange64(0xffffffffffffffffUL, 4));
        Assert.AreEqual(0x7fffffffffffffffUL, Hash.FastRange64(0xffffffffffffffffUL, 0x8000000000000000UL));
    }
}
