using System.IO;

namespace MessageVault.Files {

    public sealed class FileCheckpointReader : ICheckpointReader {
        readonly FileInfo _info;
        FileStream _stream;
        BinaryReader _reader;
        public FileCheckpointReader(FileInfo info) {
            _info = info;
        }

        bool OpenIfExists()
        {
            if (_stream != null) {
                return true;
            }
            _info.Refresh();
            if (!_info.Exists) {
                return false;
            }
            _stream = _info.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            _reader = new BinaryReader(_stream);
            return true;

        }
        public long Read() {
            if (!OpenIfExists()) {
                return 0;
            }
            _stream.Seek(0, SeekOrigin.Begin);
            return _reader.ReadInt64();

        }
    }

}