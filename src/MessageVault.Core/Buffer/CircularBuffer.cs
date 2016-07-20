using System;
using System.IO;
using MessageVault.Files;

namespace MessageVault.Buffer {

	/// <summary>
	///     Is a  FIFO queue of a fixed size. You can concurrently add bytes to it
	///     and consume them (from a single process). Queue reads from a buffer but writes
	///     also to a disk.
	/// </summary>
	public sealed class CircularBuffer : IDisposable {
		readonly string _prefixPath;
		readonly int _maxSize;
		FileStream _writeStream;

		FileCheckpointWriter _writePosition;
		FileCheckpointWriter _readPosition;

		CircularStream _durableWriter;
		CircularStream _bufferReader;

		// we need a backing buffer for fast reads
		readonly byte[] _buffer;


		CircularBuffer(string prefixPath, int maxSize) {
			_prefixPath = prefixPath;
			_maxSize = maxSize;


			_buffer = new byte[maxSize];
		}

		public static CircularBuffer Create(string prefixPath, int maxSize) {
			var queue = new CircularBuffer(prefixPath, maxSize);
			queue.InitOrCreate();
			return queue;
		}


		void InitOrCreate() {
			var queue = new FileInfo(_prefixPath + ".queue");
			var put = new FileInfo(_prefixPath + ".put");
			var get = new FileInfo(_prefixPath + ".get");

			_writeStream = queue.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);


			EnsureStreamSize();

			// initialize the local buffer
			using (var mem = new MemoryStream(_buffer)) {
				_writeStream.CopyTo(mem);
			}


			_writePosition = new FileCheckpointWriter(put);
			var writePos = _writePosition.GetOrInitPosition();
			_readPosition = new FileCheckpointWriter(get);
			var readPos = _readPosition.GetOrInitPosition();

			_durableWriter = new CircularStream(_writeStream, _maxSize);
			_bufferReader = new CircularStream(new MemoryStream(_buffer), _maxSize);
			_bufferWriter = new CircularStream(new MemoryStream(_buffer), _maxSize);

			_durableWriter.Seek(writePos);
			_bufferWriter.Seek(writePos);
			_bufferReader.Seek(readPos);
		}

		void EnsureStreamSize() {
			if (_writeStream.Length == _maxSize) {
				return;
			}
			if (_writeStream.Length != 0) {
				throw new InvalidOperationException("Stream length was already set");
			}
			_writeStream.SetLength(_maxSize);
		}

		long GetFreeSpace() {
			var writePos = _writePosition.ReadPositionVolatile();
			var readPos = _readPosition.ReadPositionVolatile();

			return _maxSize - (writePos - readPos);
		}

		readonly object _writerLock = new object();
		CircularStream _bufferWriter;

		public void Append(Stream data, int count) {
			var required = data.Length;


			// we need to lock before checking free space to avoid 
			// multiple writers getting preapproval at the same time

			lock (_writerLock) {
				var freeSpace = GetFreeSpace();
				if (required > freeSpace) {
					var message = string.Format("Need {0} bytes to enqueue but have only {1}", required, freeSpace);
					throw new InsufficientMemoryException(message);
				}

				data.Seek(0, SeekOrigin.Begin);
				_bufferWriter.Write(data, count);

				data.Seek(0, SeekOrigin.Begin);
				_durableWriter.Write(data, count);
				_durableWriter.Flush();

				var current = _writePosition.ReadPositionVolatile();
				_writePosition.Update(current + count);
			}
		}


		public int Consume<T>(Func<int, T> allocate, Action<T> handler) where T : Stream {
			var writePos = _writePosition.ReadPositionVolatile();
			var readPos = _readPosition.ReadPositionVolatile();
			var occupied = (int) (writePos - readPos);
			if (occupied == 0) {
				return 0;
			}

			using (var allocated = allocate(occupied)) {
				_bufferReader.Read(allocated, occupied);
				handler(allocated);
			}
			_readPosition.Update(writePos);
			return occupied;
		}


		public void Dispose() {
			_writeStream.Dispose();
			_writePosition.Dispose();
			_readPosition.Dispose();
		}
	}

}