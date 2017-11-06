using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

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
			//Console.WriteLine("OPEN R {0}", _info.FullName);
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

	    public Task<long> ReadAsync(CancellationToken token) {
			if (!OpenIfExists())
			{
				return Task.FromResult(0L);
			}
			_stream.Seek(0, SeekOrigin.Begin);
		    var position = _reader.ReadInt64();
		    return Task.FromResult(position);
		}

	    bool _disposed;
        public void Dispose() {
            if (_disposed) {
                return;
            }
            using (_stream)
            using (_reader) {
                _disposed = true;
				//Console.WriteLine("CLOSE R {0}", _info.FullName);
			}
        }
    }

}