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

        bool _disposed;
        public void Dispose() {
            if (_disposed) {
                return;
            }
            using (_stream)
            using (_reader) {
                _disposed = true;
            }
        }
    }
	public sealed class FileCheckpointArrayReader
	{
		readonly FileInfo _info;
		readonly int _count;
		FileStream _stream;
		BinaryReader _reader;
		public FileCheckpointArrayReader(FileInfo info, int count) {
			_info = info;
			_count = count;
		}

		bool OpenIfExists()
		{
			if (_stream != null)
			{
				return true;
			}
			_info.Refresh();
			if (!_info.Exists)
			{
				return false;
			}
			_stream = _info.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
			_reader = new BinaryReader(_stream);
			return true;

		}
		public long[] Read()
		{
			if (!OpenIfExists())
			{
				return new long[_count];
			}
			_stream.Seek(0, SeekOrigin.Begin);

			var result = new long[_count];
			for (int i = 0; i < _count; i++) {
				result[i] = _reader.ReadInt64();
			}

			return result;

		}

		bool _disposed;
		public void Dispose()
		{
			if (_disposed)
			{
				return;
			}
			using (_stream)
			using (_reader)
			{
				_disposed = true;
			}
		}
	}

}