using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using MessageVault.Files;

namespace MessageVault.Queue {


	sealed class OuroborosStream {
		readonly Stream _inner;
		readonly int _maxLength;

		public OuroborosStream(Stream inner, int maxLength) {
			_inner = inner;
			_maxLength = maxLength;
		
		
		}
		

		public void Flush() {
			var fileStream = _inner as FileStream;

			if (fileStream != null) {
				fileStream.Flush(true);
			} else {
				_inner.Flush();
			}
		}

		public void Seek(long offset) {
			var pos = offset%_maxLength;
			_inner.Seek(pos, SeekOrigin.Begin);
			
		}


		public  int Read(Stream target, int count) {
			if (count > _maxLength) {
				throw new ArgumentOutOfRangeException("count", count, "Must be less than " + _maxLength);
			}
			var pos = _inner.Position;
			var block1 = (int)(_maxLength - pos);
			if (block1 > 0) {
				var amount = (int)Math.Min(count, block1);
				CopyStream(_inner, target, amount);
				count -= amount;

				RewindIfNeeded();
			}

			if (count > 0) {
				CopyStream(_inner, target, count);
				RewindIfNeeded();
			}
			return count;
		}

		void RewindIfNeeded() {
			if (_inner.Position == _maxLength) {
				_inner.Seek(0, SeekOrigin.Begin);
			}
		}

		

		 void CopyStream(Stream input, Stream output, int bytes)
		{
			
			int read;
			while (bytes > 0 &&
				   (read = input.Read(_buffer, 0, Math.Min(_buffer.Length, bytes))) > 0)
			{
				output.Write(_buffer, 0, read);
				bytes -= read;
			}
		}

		byte[] _buffer = new byte[32768];

		public  void Write(Stream source, int count) {
			if (count > _maxLength)
			{
				throw new ArgumentOutOfRangeException("count", count, "Must be less than " + _maxLength);
			}
			var pos = _inner.Position;
			var block1 = (int)(_maxLength - pos);
			if (block1 > 0)
			{
				var amount = Math.Min(count, block1);
				CopyStream(source, _inner, amount);
				count -= amount;

				RewindIfNeeded();
			}

			if (count > 0)
			{
				CopyStream(source, _inner, count);
				RewindIfNeeded();
			}
		}
	}

	/// <summary>
	/// Is a  FIFO queue of a fixed size. You can concurrently add bytes to it
	/// and consume them (from a single process). Queue reads from a buffer but writes
	/// also to a disk. 
	/// </summary>
	public sealed class CircularQueue : IDisposable {
		
		readonly string _prefixPath;
		readonly int _maxSize;
		FileStream _writeStream;

		FileCheckpointWriter _writePosition;
		FileCheckpointWriter _readPosition;

		OuroborosStream _durableWriter;
		OuroborosStream _bufferReader;

		// we need a backing buffer for fast reads
		readonly byte[] _buffer;


		 CircularQueue(string prefixPath, int maxSize) {
			_prefixPath = prefixPath;
			_maxSize = maxSize;


			_buffer = new byte[maxSize];
		}

		public static CircularQueue Create(string prefixPath, int maxSize) {
			var queue = new CircularQueue(prefixPath, maxSize);
			queue.InitOrCreate();
			return queue;
		}


		void InitOrCreate() {
			var queue = new FileInfo(_prefixPath + ".queue");
			var put= new FileInfo(_prefixPath + ".put");
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

			_durableWriter = new OuroborosStream(_writeStream, _maxSize);
			_bufferReader = new OuroborosStream(new MemoryStream(_buffer), _maxSize);
			_bufferWriter = new OuroborosStream(new MemoryStream(_buffer),_maxSize );

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
		OuroborosStream _bufferWriter;

		public void Append(Stream data, int count) {
			
			var required = data.Length;
			

			// we need to lock before checking free space to avoid 
			// multiple writers getting preapproval at the same time

			lock (_writerLock) {
				var freeSpace = GetFreeSpace();
				if (required > freeSpace)
				{
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

		

		public int Consume<T>(Func<int,T> allocate, Action<T> handler) where T : Stream{
			var writePos = _writePosition.ReadPositionVolatile();
			var readPos = _readPosition.ReadPositionVolatile();
			var occupied = (int)(writePos - readPos);
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