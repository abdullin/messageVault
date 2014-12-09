using System;
using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault {

	public sealed class PageWriter {
		// 4MB, Azure limit
		const long CommitSizeBytes = 1024 * 1024 * 4;

		// Azure limit
		const int PageSize = 512;
		readonly CloudPageBlob _blob;

		public PageWriter(CloudPageBlob blob) {
			_blob = blob;
		}


		public long BlobSize { get; private set; }


		public static long NextSize(long current) {
			// Azure doesn't charge us for the page storage anyway
			const long hundredMBs = 1024 * 1024 * 100;
			return current + hundredMBs;
		}

		public void InitForWriting() {
			if (!_blob.Exists()) {
				_blob.Create(NextSize(0));
			}

			BlobSize = _blob.Properties.Length;
		}
		public void Grow() {
			_blob.Resize(NextSize(BlobSize));
		}

		public byte[] ReadPage(long offset) {
			using (var stream = _blob.OpenRead()) {
				var buffer = new byte[PageSize];
				stream.Seek(offset, SeekOrigin.Begin);
				stream.Read(buffer, 0, PageSize);
				return buffer;
			}
		}

		public void Save(Stream stream, long offset) {
			if (stream.Length % PageSize != 0) {
				var message = "Stream length must be multiple of " + PageSize;
				throw new ArgumentException(message);
			}
			if (stream.Length > CommitSizeBytes) {
				var message = "Stream can't be longer than " + CommitSizeBytes;
				throw new ArgumentException(message);
			}

			_blob.WritePages(stream,offset);
		}
	}

}