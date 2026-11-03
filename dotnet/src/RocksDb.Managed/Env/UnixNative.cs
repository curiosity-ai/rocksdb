using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace RocksDb.Managed.Env;

/// <summary>
/// Thin libc bindings used by <see cref="LocalFileSystem"/> for the POSIX behaviours .NET does not expose:
/// fdatasync, directory fsync, fallocate, sync_file_range, O_DIRECT and open-file-description (OFD) record
/// locks. Mirrors the system calls made by env/io_posix.cc and env/fs_posix.cc.
/// <para>
/// Every entry point is optional: callers check <see cref="IsLinux"/>/<see cref="IsUnix"/> and treat
/// <see cref="DllNotFoundException"/>/<see cref="EntryPointNotFoundException"/> as "unsupported", falling back
/// to portable .NET APIs (e.g. <see cref="RandomAccess.FlushToDisk"/>).
/// </para>
/// </summary>
internal static partial class UnixNative
{
    private const string LibC = "libc";

    public static readonly bool IsLinux = OperatingSystem.IsLinux();
    public static readonly bool IsUnix = OperatingSystem.IsLinux() || OperatingSystem.IsMacOS() || OperatingSystem.IsFreeBSD();

    // errno values (identical on Linux and macOS for these).
    public const int EINTR = 4;
    public const int EAGAIN = 11;
    public const int EACCES = 13;
    public const int EINVAL = 22;

    // Linux fcntl commands / lock types (asm-generic, same on x86_64, arm64, s390x, ppc64le).
    private const int F_GETFL = 3;
    private const int F_SETFL = 4;
    private const int F_OFD_SETLK = 37;
    private const short F_WRLCK = 1;
    private const short F_UNLCK = 2;
    private const int FALLOC_FL_KEEP_SIZE = 1;
    private const uint SYNC_FILE_RANGE_WRITE = 2;
    private const int O_RDONLY = 0;
    private const int O_CLOEXEC = 0x80000;

    /// <summary>O_DIRECT differs per architecture on Linux.</summary>
    private static int ODirect => RuntimeInformation.ProcessArchitecture switch
    {
        Architecture.Arm or Architecture.Arm64 => 0x10000,
        Architecture.Ppc64le => 0x20000,
        _ => 0x4000, // x86, x64, s390x, riscv64, loongarch64 (asm-generic 040000)
    };

    /// <summary>struct flock for 64-bit Linux (glibc and musl agree): type, whence, start, len, pid.</summary>
    [StructLayout(LayoutKind.Sequential)]
    private struct Flock64
    {
        public short l_type;
        public short l_whence;
        public long l_start;
        public long l_len;
        public int l_pid;
    }

    [LibraryImport(LibC, SetLastError = true)]
    private static partial int fdatasync(int fd);

    [LibraryImport(LibC, SetLastError = true)]
    private static partial int fsync(int fd);

    [LibraryImport(LibC, EntryPoint = "open", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
    private static partial int open_(string path, int flags);

    [LibraryImport(LibC, SetLastError = true)]
    private static partial int close(int fd);

    [LibraryImport(LibC, EntryPoint = "link", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
    private static partial int link_(string oldpath, string newpath);

    [LibraryImport("kernel32", EntryPoint = "CreateHardLinkW", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool CreateHardLinkW(string newFileName, string existingFileName, IntPtr securityAttributes);

    [LibraryImport(LibC, SetLastError = true)]
    private static partial int fallocate(int fd, int mode, long offset, long len);

    [LibraryImport(LibC, SetLastError = true)]
    private static partial int sync_file_range(int fd, long offset, long nbytes, uint flags);

    [LibraryImport(LibC, EntryPoint = "fcntl", SetLastError = true)]
    private static partial int fcntl_int(int fd, int cmd, int arg);

    [LibraryImport(LibC, EntryPoint = "fcntl", SetLastError = true)]
    private static unsafe partial int fcntl_ptr(int fd, int cmd, void* arg);

    private static volatile bool s_fdatasyncUnavailable;
    private static volatile bool s_ofdUnavailable;

    /// <summary>
    /// fdatasync() on Linux (data + size metadata, like PosixWritableFile::Sync), otherwise
    /// <see cref="RandomAccess.FlushToDisk"/> (fsync; F_FULLFSYNC on macOS).
    /// </summary>
    public static void DataSync(SafeFileHandle handle, string path)
    {
        if (IsLinux && !s_fdatasyncUnavailable)
        {
            bool addRef = false;
            try
            {
                handle.DangerousAddRef(ref addRef);
                int fd = (int)handle.DangerousGetHandle();
                int rc;
                do { rc = fdatasync(fd); } while (rc != 0 && Marshal.GetLastPInvokeError() == EINTR);
                if (rc != 0) throw IOErrorFromErrno("While fdatasync", path, Marshal.GetLastPInvokeError());
                return;
            }
            catch (Exception e) when (e is DllNotFoundException or EntryPointNotFoundException)
            {
                s_fdatasyncUnavailable = true;
            }
            finally
            {
                if (addRef) handle.DangerousRelease();
            }
        }
        try
        {
            RandomAccess.FlushToDisk(handle);
        }
        catch (IOException e)
        {
            throw RocksDbException.IOError("While fsync: " + path + ": " + e.Message, e);
        }
    }

    /// <summary>Opens the directory read-only and fsyncs it (PosixDirectory::Fsync). No-op off Unix.</summary>
    public static void FsyncDirectory(string path)
    {
        if (!IsUnix) return;
        int fd;
        try
        {
            fd = open_(path, O_RDONLY | (IsLinux ? O_CLOEXEC : 0));
        }
        catch (Exception e) when (e is DllNotFoundException or EntryPointNotFoundException)
        {
            return;
        }
        if (fd < 0) throw IOErrorFromErrno("While open directory", path, Marshal.GetLastPInvokeError());
        try
        {
            int rc;
            do { rc = fsync(fd); } while (rc != 0 && Marshal.GetLastPInvokeError() == EINTR);
            // Some file systems (e.g. certain FUSE/9p mounts) reject fsync on directories with EINVAL; RocksDB
            // treats directory fsync as best effort in that situation as well.
            if (rc != 0)
            {
                int errno = Marshal.GetLastPInvokeError();
                if (errno != EINVAL) throw IOErrorFromErrno("While fsync", path, errno);
            }
        }
        finally
        {
            close(fd);
        }
    }

    /// <summary>
    /// fallocate(FALLOC_FL_KEEP_SIZE) like PosixWritableFile::Allocate. Returns false when unsupported (the
    /// caller simply stops trying).
    /// </summary>
    public static bool TryAllocate(SafeFileHandle handle, long offset, long length)
    {
        if (!IsLinux || !Environment.Is64BitProcess) return false;
        bool addRef = false;
        try
        {
            handle.DangerousAddRef(ref addRef);
            int rc = fallocate((int)handle.DangerousGetHandle(), FALLOC_FL_KEEP_SIZE, offset, length);
            return rc == 0;
        }
        catch (Exception e) when (e is DllNotFoundException or EntryPointNotFoundException)
        {
            return false;
        }
        finally
        {
            if (addRef) handle.DangerousRelease();
        }
    }

    /// <summary>sync_file_range(SYNC_FILE_RANGE_WRITE) used for bytes_per_sync (PosixWritableFile::RangeSync).</summary>
    public static bool TryRangeSync(SafeFileHandle handle, long offset, long nbytes)
    {
        if (!IsLinux || !Environment.Is64BitProcess) return false;
        bool addRef = false;
        try
        {
            handle.DangerousAddRef(ref addRef);
            return sync_file_range((int)handle.DangerousGetHandle(), offset, nbytes, SYNC_FILE_RANGE_WRITE) == 0;
        }
        catch (Exception e) when (e is DllNotFoundException or EntryPointNotFoundException)
        {
            return false;
        }
        finally
        {
            if (addRef) handle.DangerousRelease();
        }
    }

    /// <summary>Turns on O_DIRECT for an open descriptor (fcntl F_SETFL). Linux only.</summary>
    public static void EnableDirectIO(SafeFileHandle handle, string path)
    {
        if (!IsLinux) ThrowHelper.NotSupported("Direct I/O is only supported on Linux");
        bool addRef = false;
        try
        {
            handle.DangerousAddRef(ref addRef);
            int fd = (int)handle.DangerousGetHandle();
            int flags = fcntl_int(fd, F_GETFL, 0);
            if (flags < 0 || fcntl_int(fd, F_SETFL, flags | ODirect) != 0)
            {
                int errno = Marshal.GetLastPInvokeError();
                if (errno == EINVAL) ThrowHelper.NotSupported("Direct I/O not supported by the file system: " + path);
                throw IOErrorFromErrno("While enabling O_DIRECT", path, errno);
            }
        }
        finally
        {
            if (addRef) handle.DangerousRelease();
        }
    }

    /// <summary>
    /// Takes an exclusive open-file-description lock over the whole file (F_OFD_SETLK, l_len = 0). OFD locks
    /// conflict with the classic fcntl(F_SETLK) record locks native RocksDB takes on LOCK (PosixFileSystem::LockFile),
    /// including ones held by a native RocksDB instance in this same process, and with OFD locks on other
    /// descriptors. Returns 0 on success, an errno on conflict/failure, or -1 when OFD locks are unavailable.
    /// </summary>
    public static unsafe int TryOfdLock(SafeFileHandle handle)
    {
        if (!IsLinux || !Environment.Is64BitProcess || s_ofdUnavailable) return -1;
        bool addRef = false;
        try
        {
            handle.DangerousAddRef(ref addRef);
            Flock64 fl = default;
            fl.l_type = F_WRLCK;
            fl.l_whence = 0; // SEEK_SET
            fl.l_start = 0;
            fl.l_len = 0;    // whole file, like PosixFileSystem::LockOrUnlock
            fl.l_pid = 0;    // required for OFD locks
            int rc = fcntl_ptr((int)handle.DangerousGetHandle(), F_OFD_SETLK, &fl);
            if (rc == 0) return 0;
            int errno = Marshal.GetLastPInvokeError();
            if (errno == EINVAL)
            {
                s_ofdUnavailable = true; // kernel < 3.15
                return -1;
            }
            return errno;
        }
        catch (Exception e) when (e is DllNotFoundException or EntryPointNotFoundException)
        {
            s_ofdUnavailable = true;
            return -1;
        }
        finally
        {
            if (addRef) handle.DangerousRelease();
        }
    }

    /// <summary>
    /// Releases a lock taken by <see cref="TryOfdLock"/> (F_OFD_SETLK with F_UNLCK; PosixFileSystem::UnlockFile also
    /// unlocks explicitly before closing). An OFD lock belongs to the open file description, which a child process
    /// forked by any thread shares until it execs (O_CLOEXEC only closes it at exec), so closing our descriptor alone
    /// does not release it while such a child exists. Unlocking removes the lock from the description itself.
    /// Errors are ignored: closing the descriptor afterwards still releases the lock once the last reference goes.
    /// </summary>
    public static unsafe void OfdUnlock(SafeFileHandle handle)
    {
        bool addRef = false;
        try
        {
            handle.DangerousAddRef(ref addRef);
            Flock64 fl = default;
            fl.l_type = F_UNLCK;
            fl.l_whence = 0; // SEEK_SET
            fl.l_start = 0;
            fl.l_len = 0;
            fl.l_pid = 0;
            _ = fcntl_ptr((int)handle.DangerousGetHandle(), F_OFD_SETLK, &fl);
        }
        finally
        {
            if (addRef) handle.DangerousRelease();
        }
    }

    // errno values for link(2) failures that mean "hard links are not possible here" (Linux / macOS).
    private const int EPERM = 1;
    private const int EXDEV = 18;
    private const int EMLINK_LINUX = 31;
    private const int EOPNOTSUPP_LINUX = 95;
    private const int EOPNOTSUPP_MAC = 45;
    private const int EEXIST = 17;
    private const int ENOENT = 2;
    // Windows error codes for CreateHardLink.
    private const int ERROR_FILE_NOT_FOUND = 2;
    private const int ERROR_PATH_NOT_FOUND = 3;
    private const int ERROR_ALREADY_EXISTS = 183;
    private const int ERROR_FILE_EXISTS = 80;

    /// <summary>
    /// Creates a hard link (PosixFileSystem::LinkFile: link(2); Windows: CreateHardLinkW). Cross-device links and
    /// file systems without hard link support throw NotSupported so callers can fall back to copying.
    /// </summary>
    public static void Link(string source, string target)
    {
        if (OperatingSystem.IsWindows())
        {
            if (CreateHardLinkW(target, source, IntPtr.Zero)) return;
            int err = Marshal.GetLastPInvokeError();
            switch (err)
            {
                case ERROR_FILE_NOT_FOUND:
                case ERROR_PATH_NOT_FOUND:
                    throw new RocksDbException(StatusCode.IOError, "IO error: while link file to " + target + ": " + source + ": No such file or directory",
                        StatusSubCode.PathNotFound);
                case ERROR_ALREADY_EXISTS:
                case ERROR_FILE_EXISTS:
                    throw RocksDbException.IOError("while link file to " + target + ": " + source + ": file exists");
                default:
                    throw RocksDbException.NotSupported("No support for hard link: " + Marshal.GetPInvokeErrorMessage(err));
            }
        }
        if (!IsUnix) ThrowHelper.NotSupported("No support for hard link on this platform");
        int rc;
        try
        {
            rc = link_(source, target);
        }
        catch (Exception e) when (e is DllNotFoundException or EntryPointNotFoundException)
        {
            throw RocksDbException.NotSupported("No support for hard link: " + e.Message);
        }
        if (rc == 0) return;
        int errno = Marshal.GetLastPInvokeError();
        if (errno == EXDEV || errno == EPERM || errno == EMLINK_LINUX || errno == EOPNOTSUPP_LINUX || errno == EOPNOTSUPP_MAC)
        {
            // PosixFileSystem::LinkFile: EXDEV / ENOTSUP -> NotSupported("No cross FS links allowed").
            throw RocksDbException.NotSupported("No cross FS links allowed: " + Marshal.GetPInvokeErrorMessage(errno));
        }
        if (errno == ENOENT)
        {
            throw new RocksDbException(StatusCode.IOError, "IO error: while link file to " + target + ": " + source + ": No such file or directory",
                StatusSubCode.PathNotFound);
        }
        throw IOErrorFromErrno("while link file to " + target, source, errno);
    }

    // ---- non-blocking page cache I/O (preadv2 / pwritev2 with RWF_NOWAIT) ----------------------------------

    private const int RWF_NOWAIT = 0x8;
    private const int ENOSYS = 38;

    [StructLayout(LayoutKind.Sequential)]
    private unsafe struct IoVec
    {
        public void* Base;
        public nuint Length;
    }

    [LibraryImport(LibC, SetLastError = true)]
    private static unsafe partial nint preadv2(int fd, IoVec* iov, int iovcnt, long offset, int flags);

    [LibraryImport(LibC, SetLastError = true)]
    private static unsafe partial nint pwritev2(int fd, IoVec* iov, int iovcnt, long offset, int flags);

    private static volatile bool s_noWaitReadUnavailable = !(OperatingSystem.IsLinux() && Environment.Is64BitProcess);
    private static volatile bool s_noWaitWriteUnavailable = !(OperatingSystem.IsLinux() && Environment.Is64BitProcess);

    /// <summary>True while <see cref="TryReadNoWait"/> may succeed (Linux, preadv2 available).</summary>
    public static bool NoWaitReadSupported => !s_noWaitReadUnavailable;

    /// <summary>True while <see cref="TryWriteNoWait"/> may succeed (Linux, pwritev2 + RWF_NOWAIT accepted).</summary>
    public static bool NoWaitWriteSupported => !s_noWaitWriteUnavailable;

    /// <summary>
    /// preadv2(RWF_NOWAIT): reads from the page cache without blocking on the device. Returns the number of bytes read
    /// (possibly short when only part of the range is cached; 0 at EOF), or -1 when the read would block, was interrupted
    /// or is unsupported (the caller then issues a regular, possibly asynchronous read). Lets async readers complete
    /// synchronously on a page cache hit instead of paying a thread pool round trip per block.
    /// </summary>
    public static unsafe int TryReadNoWait(SafeFileHandle handle, Span<byte> destination, long offset)
    {
        if (s_noWaitReadUnavailable) return -1;
        bool addRef = false;
        try
        {
            handle.DangerousAddRef(ref addRef);
            fixed (byte* p = destination)
            {
                IoVec v;
                v.Base = p;
                v.Length = (nuint)destination.Length;
                nint n = preadv2((int)handle.DangerousGetHandle(), &v, 1, offset, RWF_NOWAIT);
                if (n >= 0) return (int)n;
            }
            int errno = Marshal.GetLastPInvokeError();
            if (errno == ENOSYS || errno == EOPNOTSUPP_LINUX || errno == EINVAL) s_noWaitReadUnavailable = true;
            return -1;
        }
        catch (Exception e) when (e is DllNotFoundException or EntryPointNotFoundException)
        {
            s_noWaitReadUnavailable = true;
            return -1;
        }
        finally
        {
            if (addRef) handle.DangerousRelease();
        }
    }

    /// <summary>
    /// pwritev2(RWF_NOWAIT): a buffered write into the page cache that fails instead of blocking (dirty page
    /// throttling, block allocation on file systems that cannot do it without blocking). Returns the number of bytes
    /// written (possibly short) or -1 when the write would block or is unsupported (the caller then issues a regular
    /// write for the remainder).
    /// </summary>
    public static unsafe int TryWriteNoWait(SafeFileHandle handle, ReadOnlySpan<byte> data, long offset)
    {
        if (s_noWaitWriteUnavailable) return -1;
        bool addRef = false;
        try
        {
            handle.DangerousAddRef(ref addRef);
            fixed (byte* p = data)
            {
                IoVec v;
                v.Base = p;
                v.Length = (nuint)data.Length;
                nint n = pwritev2((int)handle.DangerousGetHandle(), &v, 1, offset, RWF_NOWAIT);
                if (n >= 0) return (int)n;
            }
            int errno = Marshal.GetLastPInvokeError();
            if (errno == ENOSYS || errno == EOPNOTSUPP_LINUX || errno == EINVAL) s_noWaitWriteUnavailable = true;
            return -1;
        }
        catch (Exception e) when (e is DllNotFoundException or EntryPointNotFoundException)
        {
            s_noWaitWriteUnavailable = true;
            return -1;
        }
        finally
        {
            if (addRef) handle.DangerousRelease();
        }
    }

    public static RocksDbException IOErrorFromErrno(string context, string path, int errno) =>
        RocksDbException.IOError(context + ": " + path + ": " + Marshal.GetPInvokeErrorMessage(errno) + " (errno " + errno + ")");
}
