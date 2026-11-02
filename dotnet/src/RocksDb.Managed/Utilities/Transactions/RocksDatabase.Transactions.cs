using RocksDb.Managed.Db;

namespace RocksDb.Managed;

public sealed partial class RocksDatabase
{
    /// <summary>The DB implementation (used by the transaction layer, which sits on top of the DB like StackableDB).</summary>
    internal DbImpl Impl => _db;
}
