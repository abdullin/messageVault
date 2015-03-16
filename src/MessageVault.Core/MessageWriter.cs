using System;
using System.Collections.Generic;
using System.IO;
using System.Text;


namespace MessageVault {

	/// <summary>
	/// Writes messages as a stream to an underlying <see cref="IPageWriter"/>, 
	/// using provided <see cref="ICheckpointWriter"/> to mark commits.
	/// </summary>
	public sealed class MessageWriter : IDisposable{
		
		readonly IPageWriter _pageWriter;
		readonly ICheckpointWriter _positionWriter;

		long _savedPosition;
		readonly int _pageSize;

		readonly byte[] _buffer;
		readonly MemoryStream _stream;
		readonly BinaryWriter _binary;

		public MessageWriter(IPageWriter pageWriter, ICheckpointWriter positionWriter) {
			_pageWriter = pageWriter;
			_positionWriter = positionWriter;
		
			_buffer = new byte[pageWriter.GetMaxCommitSize()];
			_pageSize = pageWriter.GetPageSize();
			_stream = new MemoryStream(_buffer, true);
			_binary = new BinaryWriter(_stream, Encoding.UTF8, true);
		}

		public long GetPosition() {
			return _savedPosition;
		}
		public int GetBufferSize()
		{
			return _buffer.Length;
		}

		public void Init() {
			_pageWriter.Init();
			_savedPosition = _positionWriter.GetOrInitPosition();

			//_log.Verbose("Stream {stream} at {offset}", _streamName, _position);

			var tail = TailInPage(_savedPosition);
			if (tail != 0) {
				// preload tail

				var offset = FloorInPage(_savedPosition);
				//_log.Verbose("Load tail at {offset}", offset);
				var page = _pageWriter.ReadPage(offset);
				_stream.Write(page, 0, tail);
			}
		}

		long PageCeiling(long value) {
			var tail = TailInPage(value);
			if (tail == 0) {
				return value;
			}
			return value - tail + _pageSize;
		}

		/// <summary>
		/// Calculates portion of bytes in value that can fill up entire page.
		/// </summary>
		/// <param name="value">The value.</param>
		/// <returns></returns>
		long FloorInPage(long value) {
			return value - TailInPage(value);
		}

		int TailInPage(long value) {
			return (int) (value % _pageSize);
		}

		long VirtualPosition() {
			return FloorInPage(_savedPosition) + _stream.Position;
		}

		long BufferStarts() {
			return FloorInPage(_savedPosition);
		}
		long DataStarts() {
			return _savedPosition;
		}
		long DataEnds() {
			return BufferStarts() + _stream.Position;
		}
		long BufferEnds() {
			return BufferStarts() + PageCeiling(_stream.Position);
		}

		void FlushBuffer() {
			var bytesToWrite = _stream.Position;
			

			//Log.Verbose("Flush Buffer {3}-[{0} ({2}) {1}]-{4}", 
			//	DataStarts(), 
			//	DataEnds(), 
			//	DataEnds() - DataStarts(),
			//	BufferStarts(),
			//	BufferEnds());

			// Ensure we have enough space to write everything in buffer
			var newPosition = VirtualPosition();
			_pageWriter.EnsureSize(PageCeiling(newPosition));
			
			// how many bytes do we need to write, rounded up to pages
			var pageBytesToWrite = (int) PageCeiling(_stream.Position);

			// Write all data out of buffer, including the empty tail (if present)
			// starting from the _savedPosition, rounded down to pages
			using (var copy = new MemoryStream(_buffer, 0, pageBytesToWrite)) {
				_pageWriter.Save(copy, FloorInPage(_savedPosition));
			}

			_savedPosition = newPosition;
			// we write so little, that our page tail didn't change
			if (bytesToWrite < _pageSize) {
				return;
			}
			// grab the tail of the stream (up to the page size)
			// and keep it for future writes
			var tail = TailInPage(bytesToWrite);

			if (tail == 0) {
				// our position is at the page boundary, nothing to keep
				Array.Clear(_buffer, 0, _buffer.Length);
				_stream.Seek(0, SeekOrigin.Begin);
			} else {
				// we need to save the tail in memory
				Array.Copy(_buffer, FloorInPage(bytesToWrite), _buffer, 0, _pageSize);
				Array.Clear(_buffer, _pageSize, _buffer.Length - _pageSize);
				_stream.Seek(tail, SeekOrigin.Begin);
			}
		}


		public long Append(ICollection<MessageToWrite> messages) {
			if (messages.Count == 0) {
				throw new ArgumentException("Must provide non-empty array", "messages");
			}
			foreach (var item in messages) {
				if (item.Value.Length > Constants.MaxMessageSize) {
					string message = "Each message must be smaller than " + Constants.MaxMessageSize;
					throw new InvalidOperationException(message);
				}

				if (item.Key.Length > Constants.MaxContractLength) {
					var message = "Each contract must be shorter than " + Constants.MaxContractLength;
					throw new InvalidOperationException(message);
				}

				var sizeEstimate = MessageFormat.EstimateSize(item);

				var availableInBuffer = _stream.Length - _stream.Position;
				if (sizeEstimate > availableInBuffer) {
					FlushBuffer();
				}

				var offset = VirtualPosition();
				var id = MessageId.CreateNew(offset);
				MessageFormat.Write(_binary, id, item);
			}
			FlushBuffer();
			_positionWriter.Update(_savedPosition);
			return _savedPosition;
		}

	    bool _disposed;
	    public void Dispose() {
	        if (_disposed) {
	            return;
	        }
            using (_positionWriter)
            using (_pageWriter) {
                _disposed = true;
            }

	    }
	}
}