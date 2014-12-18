using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using MessageVault.Cloud;
using Microsoft.WindowsAzure.Storage.Blob;
using Serilog;

namespace MessageVault {

	public class MessageWriter {
		// 4MB, Azure limit
		//public const long BufferSize = 1024 * 1024 * 4;
		// Azure limit
		
		readonly byte[] _buffer;
		readonly IPageWriter _pages;
		readonly ICheckpointWriter _positionWriter;
		readonly string _streamName;
		readonly int _pageSize;

		readonly MemoryStream _stream;
		readonly BinaryWriter _binary;
		long _position;

		public int GetBufferSize() {
			return _buffer.Length;
		}


		readonly ILogger _log;


		public MessageWriter(IPageWriter pages, ICheckpointWriter positionWriter, string stream) {
			_pages = pages;
			_positionWriter = positionWriter;
			_streamName = stream;
			_buffer = new byte[pages.GetMaxCommitSize()];
			_pageSize = pages.GetPageSize();
			_stream = new MemoryStream(_buffer, true);
			_binary = new BinaryWriter(_stream, Encoding.UTF8, true);
			_log = Log.ForContext<MessageWriter>();
		}

		public long Position {
			get { return _position; }
		}

		


		public void Init() {
			_pages.Init();
			_position = _positionWriter.GetOrInitPosition();

			_log.Verbose("Stream {stream} at {offset}", _streamName, _position);

			var tail = Tail(Position);
			if (tail != 0) {
				// preload tail

				var offset = Floor(Position);
				_log.Verbose("Load tail at {offset}", offset);
				var page = _pages.ReadPage(offset);
				_stream.Write(page, 0, tail);
			}
		}

		 long Ceiling(long value) {
			var tail = Tail(value);
			if (tail == 0) {
				return value;
			}
			return value - tail + _pageSize;
		}

		 long Floor(long value) {
			return value - Tail(value);
		}

		 int Tail(long value) {
			return (int) (value % _pageSize);
		}


		long VirtualPosition() {
			return Floor(Position) + _stream.Position;
		}

		void FlushBuffer() {
			var bytesToWrite = _stream.Position;

			Log.Verbose("Flush buffer with {size} at {position}",
				bytesToWrite, Floor(Position));

			var newPosition = VirtualPosition();
			Log.Verbose("Pusition change {old} => {new}", _position, newPosition);

			_pages.EnsureSize(Ceiling(newPosition));
			

			var fullBytesToWrite = (int) Ceiling(_stream.Position);

			using (var copy = new MemoryStream(_buffer, 0, fullBytesToWrite)) {
				_pages.Save(copy, Floor(Position));
			}

			_position = newPosition;
			

			if (bytesToWrite < _pageSize) {
				return;
			}

			var tail = Tail(bytesToWrite);

			if (tail == 0) {
				Array.Clear(_buffer, 0, _buffer.Length);
				_stream.Seek(0, SeekOrigin.Begin);
			} else {
				Array.Copy(_buffer, Floor(bytesToWrite), _buffer, 0, _pageSize);
				Array.Clear(_buffer, _pageSize, _buffer.Length - _pageSize);
				_stream.Seek(tail, SeekOrigin.Begin);
			}
		}


		public long Append(ICollection<IncomingMessage> messages) {
			foreach (var item in messages) {
				var chunk = item.Data;
				if (chunk.Length > Constants.MaxMessageSize) {
					string message = "Each message must be smaller than " + Constants.MaxMessageSize;
					throw new InvalidOperationException(message);
				}

				if (item.Contract.Length > Constants.MaxContractLength) {
					var message = "Each contract must be shorter than " + Constants.MaxContractLength;
					throw new InvalidOperationException(message);
				}

				
				
				int sizeEstimate = 4 + chunk.Length + 2 * item.Contract.Length + 5;
				if (sizeEstimate + _stream.Position >= _stream.Length) {
					FlushBuffer();
				}
				var offset = VirtualPosition();
				var id = MessageId.CreateNew(offset);
				_binary.Write(Constants.ReservedFormatVersion);
				_binary.Write(id.GetBytes());
				_binary.Write(item.Contract);
				_binary.Write(chunk.Length);
				_binary.Write(chunk);
			}
			FlushBuffer();
			_positionWriter.Update(Position);
			return Position;
		}
	}

}