using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;
using Serilog;

namespace MessageVault {

	public class SegmentWriter {
		// 4MB, Azure limit
		public const long BufferSize = 1024 * 1024 * 4;
		// this would allow consumers to use fixed-size buffers.
		// Also see Snappy framing format
		// https://code.google.com/p/snappy/source/browse/trunk/framing_format.txt
		const long MaxMessageSize = 65536;
		// Azure limit
		const int PageSize = 512;
		readonly byte[] _buffer = new byte[BufferSize];
		readonly PageWriter _pages;
		readonly PositionWriter _positionWriter;
		readonly string _streamName;

		readonly MemoryStream _stream;
		long _position;

		public static SegmentWriter Create(CloudBlobClient client, string stream) {
			var container = client.GetContainerReference(stream);
			container.CreateIfNotExists();
			var dataBlob = container.GetPageBlobReference("stream.dat");
			var posBlob = container.GetPageBlobReference("stream.chk");
			var pageWriter = new PageWriter(dataBlob);
			var posWriter = new PositionWriter(posBlob);
			var writer = new SegmentWriter(pageWriter, posWriter, stream);
			writer.Init();

			return writer;
		}

		readonly ILogger _log;


		SegmentWriter(PageWriter pages, PositionWriter positionWriter, string stream) {
			_pages = pages;
			_positionWriter = positionWriter;
			_streamName = stream;

			_stream = new MemoryStream(_buffer, true);
			_log = Log.ForContext<SegmentWriter>();
		}

		public long Position {
			get { return _position; }
		}

		public long BlobSize {
			get { return _pages.BlobSize; }
		}


		public void Init() {
			_pages.InitForWriting();
			_position = _positionWriter.GetOrInitPosition();

			_log.Verbose("Init stream {stream}, {size} at {offset}", _streamName, _pages.BlobSize, _position);

			var tail = Tail(Position);
			if (tail != 0) {
				// preload tail

				var offset = Floor(Position);
				_log.Verbose("Load tail at {offset}", offset);
				var page = _pages.ReadPage(offset);
				_stream.Write(page, 0, tail);
			}
		}

		static long Ceiling(long value) {
			var tail = Tail(value);
			if (tail == 0) {
				return value;
			}
			return value - tail + PageSize;
		}

		static long Floor(long value) {
			return value - Tail(value);
		}

		static int Tail(long value) {
			return (int) (value % PageSize);
		}


		void FlushBuffer() {
			var bytesToWrite = _stream.Position;

			Log.Verbose("Flush buffer with {size} at {position}",
				bytesToWrite, Floor(Position));

			var newPosition = Floor(Position) + _stream.Position;
			Log.Verbose("Pusition change {old} => {new}", _position, newPosition);
			while (newPosition >= _pages.BlobSize) {
				_pages.Grow();
			}

			var fullBytesToWrite = (int) Ceiling(_stream.Position);

			using (var copy = new MemoryStream(_buffer, 0, fullBytesToWrite)) {
				_pages.Save(copy, Floor(Position));
			}

			_position = newPosition;
			

			if (bytesToWrite < PageSize) {
				return;
			}

			var tail = Tail(bytesToWrite);

			if (tail == 0) {
				Array.Clear(_buffer, 0, _buffer.Length);
				_stream.Seek(0, SeekOrigin.Begin);
			} else {
				Array.Copy(_buffer, Floor(bytesToWrite), _buffer, 0, PageSize);
				Array.Clear(_buffer, PageSize, _buffer.Length - PageSize);
				_stream.Seek(tail, SeekOrigin.Begin);
			}
		}


		public void Append(IEnumerable<byte[]> data) {
			foreach (var chunk in data) {
				if (chunk.Length > MaxMessageSize) {
					string message = "Each message must be smaller than " + MaxMessageSize;
					throw new InvalidOperationException(message);
				}

				int newBlock = 4 + chunk.Length;
				if (newBlock + _stream.Position >= _stream.Length) {
					FlushBuffer();
				}
				_stream.Write(BitConverter.GetBytes(chunk.Length), 0, 4);
				_stream.Write(chunk, 0, chunk.Length);
			}
			FlushBuffer();
			_positionWriter.Update(Position);
		}
	}

}