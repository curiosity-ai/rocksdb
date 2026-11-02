using System.Collections.Concurrent;
using System.Diagnostics;

namespace RocksDb.Managed.Db;

/// <summary>
/// Named callbacks invoked at interesting points of the DB implementation, for tests that need to pause a background
/// job or inject a failure at a precise interleaving. A lite port of the TEST_SYNC_POINT / SyncPoint mechanism
/// (test_util/sync_point.h): calls are compiled only in DEBUG builds, so release builds pay nothing.
/// Hook names follow upstream's "Class::Method:Point" style.
/// </summary>
internal static class TestHooks
{
#if DEBUG
    private static readonly ConcurrentDictionary<string, Action> s_hooks = new(StringComparer.Ordinal);
    private static volatile bool s_any;
#endif

    /// <summary>Installs (or with null removes) the callback for <paramref name="name"/>. DEBUG builds only.</summary>
    public static void Set(string name, Action? callback)
    {
#if DEBUG
        if (callback is null) s_hooks.TryRemove(name, out _);
        else s_hooks[name] = callback;
        s_any = !s_hooks.IsEmpty;
#endif
    }

    /// <summary>Removes every callback.</summary>
    public static void ClearAll()
    {
#if DEBUG
        s_hooks.Clear();
        s_any = false;
#endif
    }

    /// <summary>Runs the callback registered for <paramref name="name"/>, if any (TEST_SYNC_POINT).</summary>
    [Conditional("DEBUG")]
    public static void Invoke(string name)
    {
#if DEBUG
        if (s_any && s_hooks.TryGetValue(name, out Action? a)) a();
#endif
    }
}
