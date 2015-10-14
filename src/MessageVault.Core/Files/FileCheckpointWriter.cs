using System.IO;
using System.Text;

namespace MessageVault.Files {

    public class FileCheckpointWriter : ICheckpointWriter {

        readonly FileInfo _info;
        FileStream _stream;
        BinaryWriter _writer;
        public FileCheckpointWriter(FileInfo info) {
            _info = info;
        }


        public long GetOrInitPosition() {
            
            if (!_info.Exists) {
                _stream = _info.Open(FileMode.Create, FileAccess.ReadWrite, FileShare.Read);
                _writer = new BinaryWriter(_stream);
                _writer.Write((long)(0));
                _stream.Flush();
                return 0;
            }
            _stream = _info.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            _writer = new BinaryWriter(_stream);

            using (var read = new BinaryReader(_stream,Encoding.UTF8, true)) {
                return read.ReadInt64();
            }
        }

        public void Update(long position) {
            _stream.Seek(0, SeekOrigin.Begin);
            _writer.Write(position);
            _stream.Flush();
        }

        bool _disposed;
        public void Dispose() {
            if (_disposed) {
                return;
            }
            using (_stream)
            using (_writer) {
                _disposed = true;
            }
        }
    }

}