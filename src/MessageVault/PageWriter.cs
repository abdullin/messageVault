using System;
using System.IO;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault {

	public sealed class PageWriter {
		// 4MB, Azure limit
		const long CommitSizeBytes = 1024 * 1024 * 4;

		// Azure limit
		const int PageSize = 512;
		readonly CloudPageBlob _blob;
		string _etag;

		public PageWriter(CloudPageBlob blob) {
			_blob = blob;
		}


		public long BlobSize { get; private set; }


		public static long NextSize(long size) {
			Require.OffsetMultiple("size", size, PageSize);
			// Azure doesn't charge us for the page storage anyway
			const long hundredMBs = 1024 * 1024 * 100;
			return size + hundredMBs;
		}

		public void InitForWriting() {
			if (!_blob.Exists()) {
				//var nextSize = NextSize(0);
				_blob.Create(0, AccessCondition.GenerateIfNoneMatchCondition("*"));
			}

			BlobSize = _blob.Properties.Length;
			_etag = _blob.Properties.ETag;
		}

		public void EnsureSize(long size) {
			
			Require.OffsetMultiple("size", size, PageSize);
			var current = _blob.Properties.Length;
			if (size <= current) {
				return;
			}
			while (size < current) {
				size = NextSize(size);
			}

			_blob.Resize(NextSize(BlobSize), AccessCondition.GenerateIfMatchCondition(_etag));
			_etag = _blob.Properties.ETag;

		}
		

		public byte[] ReadPage(long offset) {
			Require.OffsetMultiple("offset", offset, PageSize);
			

			using (var stream = _blob.OpenRead()) {
				var buffer = new byte[PageSize];
				stream.Seek(offset, SeekOrigin.Begin);
				stream.Read(buffer, 0, PageSize);
				return buffer;
			}
		}

		public void Save(Stream stream, long offset) {
			Require.OffsetMultiple("offset", offset, PageSize);
			
			if (stream.Length > CommitSizeBytes) {
				var message = "Stream can't be longer than " + CommitSizeBytes;
				throw new ArgumentException(message);
			}

			_blob.WritePages(stream,offset, accessCondition:AccessCondition.GenerateIfMatchCondition(_etag));
			_etag = _blob.Properties.ETag;
		}
	}

}