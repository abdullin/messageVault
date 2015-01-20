using System;
using System.IO;

namespace MessageVault.Files {

    public class FilePageWriter : IPageWriter, IDisposable {
        FileStream _stream;
        readonly FileInfo _info;
        long _size;

        public void Init() {
            _info.Refresh();
            if (!_info.Exists) {
                _stream = _info.Create();
            } else {
                _stream = _info.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            }
            _size = _info.Length;
        }

        const int PageSize = 1024;

        static long NextSize(long size) {
            Require.OffsetMultiple("size", size, PageSize);
            // Azure doesn't charge us for the page storage anyway
            const long tenMBs = 1024*1024*10;
            return size + tenMBs;
        }

        public void EnsureSize(long size) {
            Require.OffsetMultiple("size", size, PageSize);
            var current = _size;
            if (size <= current) {
                return;
            }
            while (size < current) {
                size = NextSize(size);
            }
            _stream.SetLength(size);
        }

        public byte[] ReadPage(long offset) {
            _stream.Seek(offset, SeekOrigin.Begin);
            var buffer = new Byte[PageSize];
            _stream.Read(buffer, 0, PageSize);
            return buffer;
        }

        public void Save(Stream stream, long offset) {
            _stream.Seek(offset, SeekOrigin.Begin);
            stream.CopyTo(_stream);
            _stream.Flush();
        }

        public int GetMaxCommitSize() {
            return 4*1024*1024;
        }

        public int GetPageSize() {
            return PageSize;
        }

        bool _disposed;
        public FilePageWriter(FileInfo info) {
            _info = info;
        }

        public void Dispose() {
            if (_disposed) {
                return;
            }
            using (_stream) {
                _disposed = true;
                _stream.Close();
            }
        }
    }

}