using System;
using System.IO;

namespace MessageVault.Buffer {

	/// <summary>
	/// Just like ouroboros, it eats its own tail
	/// </summary>
	sealed class CircularStream {
		readonly Stream _inner;
		readonly int _maxLength;

		public CircularStream(Stream inner, int maxLength) {
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

		readonly byte[] _buffer = new byte[32768];

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

}