using System.IO;

namespace MessageVault.Files {

    public class FileCheckpointWriter : ICheckpointWriter {

        readonly FileInfo _info;
        FileStream _stream;
        BinaryWriter _writer;
        public FileCheckpointWriter(FileInfo info) {
            _info = info;
        }

        public long GetOrInitPosition() {
            if (!_info.Exists)
            {
                _stream = _info.Create();
            }
            else
            {
                _stream = _info.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
                
            }

            _writer = new BinaryWriter(_stream);

            using (var read = new BinaryReader(_stream)) {
                return read.ReadInt64();
            }
        }

        public void Update(long position) {
            _stream.Seek(0, SeekOrigin.Begin);
            _writer.Write(position);
            _stream.Flush();
        }
    }

}