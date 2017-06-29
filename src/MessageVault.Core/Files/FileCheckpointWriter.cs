using System;
using System.IO;
using System.Text;
using System.Threading;

namespace MessageVault.Files {

    public class FileCheckpointWriter : ICheckpointWriter {

        readonly FileInfo _info;
        FileStream _stream;
        BinaryWriter _writer;
	    long _position;
        public FileCheckpointWriter(FileInfo info) {
            _info = info;
        }


        public long GetOrInitPosition() {
	        if (_disposed) {
		        throw new ObjectDisposedException(nameof(FileCheckpointWriter));
	        }

	        if (_stream != null) {
		        return ReadPositionVolatile();
	        }
            
            if (!_info.Exists) {
				//Console.WriteLine("OPEN WR {0}", _info.FullName);
				_stream = _info.Open(FileMode.Create, FileAccess.ReadWrite, FileShare.Read);
                _writer = new BinaryWriter(_stream);
                _writer.Write((long)(0));
                _stream.Flush();
				Thread.VolatileWrite(ref _position, 0);
                return 0;
            }
			//Console.WriteLine("OPEN WR {0}", _info.FullName);
			_stream = _info.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            _writer = new BinaryWriter(_stream);

            using (var read = new BinaryReader(_stream,Encoding.UTF8, true)) {
	            var position = read.ReadInt64();
				Thread.VolatileWrite(ref _position, position);
	            return position;
            }
        }

        public void Update(long position) {
			Thread.VolatileWrite(ref _position, position);
            _stream.Seek(0, SeekOrigin.Begin);
            _writer.Write(position);
            _stream.Flush();
        }

	    public long ReadPositionVolatile() {
		    return Thread.VolatileRead(ref _position);
	    }

        bool _disposed;
        public void Dispose() {
            if (_disposed) {
                return;
            }
            using (_stream)
            using (_writer) {
				Console.WriteLine("CLOSE WR {0}", _info.FullName);
				_disposed = true;
            }
        }
    }

	public interface IVolatileCheckpointVectorAccess {
		long[] ReadPositionVolatile();
	}
	public class FileCheckpointArrayWriter :IVolatileCheckpointVectorAccess
	{

		readonly FileInfo _info;
		readonly int _count;
		FileStream _stream;
		BinaryWriter _writer;
		long[] _position;
		public FileCheckpointArrayWriter(FileInfo info, int count) {
			_info = info;
			_count = count;
			_position = new long[_count];
		}

	


		public long[] GetOrInitPosition()
		{
			if (!_info.Exists)
			{
				_stream = _info.Open(FileMode.Create, FileAccess.ReadWrite, FileShare.Read);
				_writer = new BinaryWriter(_stream);
				for (int i = 0; i < _count; i++) {
					_writer.Write((long)(0));
				}
				
				_stream.Flush();
				_position = new long[_count];
				return _position;
			}
			_stream = _info.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
			_writer = new BinaryWriter(_stream);

			using (var read = new BinaryReader(_stream, Encoding.UTF8, true)) {
				var position = new long[_count];
				for (int i = 0; i < _count; i++) {
					position[i] = read.ReadInt64();
				}
				
				_position = position;
				return position;
			}
		}

		public void Update(long[] position)
		{
			_stream.Seek(0, SeekOrigin.Begin);
			for (int i = 0; i < _count; i++) {
				_writer.Write(position[i]);
			}
			_stream.Flush(true);
			_position = position;
		}

		public long[] ReadPositionVolatile()
		{
			return _position;
		}

		bool _disposed;
		public void Dispose()
		{
			if (_disposed)
			{
				return;
			}
			using (_stream)
			using (_writer)
			{
				_disposed = true;
			}
		}
	}

}