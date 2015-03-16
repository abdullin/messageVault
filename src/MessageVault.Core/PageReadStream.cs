using System;
using System.IO;

namespace MessageVault {

	/// <summary>
	/// Non-seekable read-only stream which pre-fetches data from undelying page storage
	/// </summary>
	public sealed class PageReadStream : Stream {
		readonly IPageReader _reader;
		 long _max;
		readonly byte[] _buffer;
		long _position;
		
		readonly MemoryStream _mem;

		public PageReadStream(IPageReader reader, long start, long max, byte[] buffer) {
			_reader = reader;
			
			_max = max;
			_buffer = buffer;
			_position = start;
			_mem = new MemoryStream(buffer);
			_mem.SetLength(0);
		}

		public override void Flush() {
			throw new NotSupportedException();
		}

		public override long Seek(long offset, SeekOrigin origin) {
			throw new NotSupportedException();
		}

		public override void SetLength(long value) {
		    _max = value;
		}

		public override int Read(byte[] buffer, int offset, int count) {
			Require.NotNull("buffer", buffer);
			Require.ZeroOrGreater("offset", offset);
			Require.Positive("count", count);

			var remainInBuffer = _mem.Length - _mem.Position;
			if (count > remainInBuffer) {
				PreLoad(count);
			}
			var read = _mem.Read(buffer, offset, count);
			_position += read;
			return read;
		}

		void PreLoad(int count) {
			var remainInBuffer = (int)(_mem.Length - _mem.Position);
			var buffer = _buffer;
			if (remainInBuffer > 0) {
				Array.Copy(buffer, _mem.Position, buffer, 0, remainInBuffer);
			}
			_mem.SetLength(remainInBuffer);
			_mem.Seek(remainInBuffer, SeekOrigin.Begin);

			var downloadFrom = _position + remainInBuffer;
			var availableInStream = _max - downloadFrom;
			var bufferSize = _buffer.Length;
			var download = Math.Min(bufferSize - remainInBuffer, availableInStream);
			if (count > (remainInBuffer + download)) {
				var message = string.Format(
					"Buffer is too small. Cached {0}, requested {1}, cap {2}", 
					remainInBuffer, count, 
					_buffer.Length);
				throw new InvalidOperationException(message);
			}
			_reader.DownloadRangeToStream(_mem, downloadFrom, (int) download);
			_mem.Seek(0, SeekOrigin.Begin);
			
		}

		public override void Write(byte[] buffer, int offset, int count) {
			throw new NotSupportedException();
		}

		public override bool CanRead {
			get { return true; }
		}

		public override bool CanSeek {
			get { return false; }
		}

		public override bool CanWrite {
			get { return false; }
		}

		public override long Length {
			get { return _max; }
		}

		public override long Position { get { return _position; }
			set { throw new InvalidOperationException("Can't seek on this stream"); }
		}
	}
}