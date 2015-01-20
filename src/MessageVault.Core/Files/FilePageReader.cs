using System;
using System.IO;

namespace MessageVault.Files {

    public class FilePageReader : IPageReader, IDisposable {

        readonly FileInfo _info;
        FileStream _stream;

        byte[] _buffer = new byte[1024*1024];
        public FilePageReader(FileInfo info) {
            _info = info;
        }

        public void DownloadRangeToStream(Stream stream, long offset, int length) {
            OpenStreamIfNeeded();

            _stream.Seek(offset, SeekOrigin.Begin);
            
            var bytesToCopy = length;
            while (bytesToCopy > 0) {
                var read = _stream.Read(_buffer, 0, Math.Min(_buffer.Length, bytesToCopy));
                stream.Write(_buffer, 0, read);
                bytesToCopy -= read;
            }
            
        }

        void OpenStreamIfNeeded() {
            if (_stream == null) {
                _info.Refresh();
                if (!_info.Exists) {
                    const string s = "Trying to read from non-existent file";
                    throw new InvalidOperationException(s);
                }
                _stream = _info.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            }
        }

        bool _disposed;
        public void Dispose() {
            if (_disposed) {
                return;
            }
            if (_stream == null) {
                _disposed = true;
                return;
            }
            using (_stream) {
                _stream.Close();
                _disposed = true;
            }
            
        }
    }

}