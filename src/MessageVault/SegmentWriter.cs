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

		readonly MemoryStream _stream;
		long _position;

		public static SegmentWriter Create(CloudBlobContainer container, string stream) {
			var dataBlob = container.GetPageBlobReference(stream + ".dat");
			var posBlob = container.GetPageBlobReference(stream + ".chk");
			var pageWriter = new PageWriter(dataBlob);
			var posWriter = new PositionWriter(posBlob);
			var writer = new SegmentWriter(pageWriter, posWriter);
			writer.Init();
			return writer;

		}


		SegmentWriter(PageWriter pages, PositionWriter positionWriter) {
			_pages = pages;
			_positionWriter = positionWriter;

			_stream = new MemoryStream(_buffer, true);
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

			if (PartialPage(Position)) {
				// preload tail
				byte[] page = _pages.ReadPage(Floor(Position));
				_stream.Write(page, 0, page.Length);
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

		static long Tail(long value) {
			return value % PageSize;
		}

		static bool PartialPage(long value) {
			return value % PageSize != 0;
		}

		void FlushBuffer() {
			var bytesToWrite = _stream.Position;

			Log.Verbose("Flush buffer with {size} at {position}",
				bytesToWrite, Position);

			var newPosition = Floor(Position) + _stream.Position;
			while (newPosition >= _pages.BlobSize) {
				_pages.Grow();
			}


			var fullBytesToWrite = (int) Ceiling(_stream.Position);

			using (var copy = new MemoryStream(_buffer, 0, fullBytesToWrite)) {
				_pages.Save(copy, Floor(Position));
			}

			_position = newPosition;
			_positionWriter.Update(Position);

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
			
		}
	}

}