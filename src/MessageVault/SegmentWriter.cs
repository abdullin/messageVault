using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.WindowsAzure.Storage.Blob;
using Serilog;

namespace MessageVault {

	public class SegmentWriter {
		// 4MB, Azure limit
		public const long BufferSize = 1024 * 1024 * 4;
		// Azure limit
		const int PageSize = 512;
		readonly byte[] _buffer = new byte[BufferSize];
		readonly PageWriter _pages;
		readonly PositionWriter _positionWriter;
		readonly string _streamName;
		readonly CloudBlobContainer _container;

		readonly MemoryStream _stream;
		readonly BinaryWriter _binary;
		long _position;

		

		public static SegmentWriter Create(CloudBlobClient client, string stream) {
			var container = client.GetContainerReference(stream);
			container.CreateIfNotExists();
			var dataBlob = container.GetPageBlobReference(Constants.StreamFileName);
			var posBlob = container.GetPageBlobReference(Constants.PositionFileName);
			var pageWriter = new PageWriter(dataBlob);
			var posWriter = new PositionWriter(posBlob);
			var writer = new SegmentWriter(pageWriter, posWriter, stream, container);
			writer.Init();

			return writer;
		}

		readonly ILogger _log;


		SegmentWriter(PageWriter pages, PositionWriter positionWriter, string stream, CloudBlobContainer container) {
			_pages = pages;
			_positionWriter = positionWriter;
			_streamName = stream;
			_container = container;

			_stream = new MemoryStream(_buffer, true);
			_binary = new BinaryWriter(_stream, Encoding.UTF8, true);
			_log = Log.ForContext<SegmentWriter>();
		}

		public long Position {
			get { return _position; }
		}

		//public long BlobSize {
		//	get { return _pages.BlobSize; }
		//}


		public void Init() {
			_pages.InitForWriting();
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

		public static string GetReadAccessSignature(CloudBlobClient client, string stream) {
			var container = client.GetContainerReference(stream);
			var signature = container.GetSharedAccessSignature(new SharedAccessBlobPolicy {
				Permissions = SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Read, 
				SharedAccessExpiryTime = DateTimeOffset.Now.AddDays(7),
			});
			return container.Uri + signature;
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