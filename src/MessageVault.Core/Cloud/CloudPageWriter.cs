using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault.Cloud {

	public sealed class CloudPageWriter : IPageWriter {
		// 4MB, Azure limit
		const int CommitSizeBytes = 1024 * 1024 * 4;

		// Azure limit
		const int PageSize = 512;
		readonly CloudPageBlob _blob;
		string _etag;
		long _size;

		public CloudPageWriter(CloudPageBlob blob) {
			_blob = blob;
		}

		public void Init() {
			if (!_blob.Exists()) {
				//var nextSize = NextSize(0);
				_blob.Create(0, AccessCondition.GenerateIfNoneMatchCondition("*"));
			}

			_size = _blob.Properties.Length;
			_etag = _blob.Properties.ETag;
		}

		public void EnsureSize(long size) {
			Require.OffsetMultiple("size", size, PageSize);

			long current = _size;
			if (size <= current) {
				return;
			}
			while (size < current) {
				size = NextSize(size);
			}

			Throw.OnEtagMismatchDuringAppend(() => {
				_blob.Resize(NextSize(_size), AccessCondition.GenerateIfMatchCondition(_etag));
				_etag = _blob.Properties.ETag;
				_size = _blob.Properties.Length;
			});
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

			Throw.OnEtagMismatchDuringAppend(() => {
				_blob.WritePages(stream, offset,
					accessCondition : AccessCondition.GenerateIfMatchCondition(_etag));
				_etag = _blob.Properties.ETag;
			});
		}


		public async Task SaveAsync(Stream stream, long offset, CancellationToken token)
		{
			Require.OffsetMultiple("offset", offset, PageSize);

			if (stream.Length > CommitSizeBytes)
			{
				var message = "Stream can't be longer than " + CommitSizeBytes;
				throw new ArgumentException(message);
			}

			try
			{
				await _blob.WritePagesAsync(stream, offset,null, 
					AccessCondition.GenerateIfMatchCondition(_etag),null,null,token)
					.ConfigureAwait(false);
				_etag = _blob.Properties.ETag;
			}
			catch (StorageException ex)
			{
				if (ex.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
				{
					throw new NonTransientAppendFailure("ETAG failed, must reboot", ex);
				}
				throw;
			}
		}

		public int GetMaxCommitSize() {
			return CommitSizeBytes;
		}

		public int GetPageSize() {
			return PageSize;
		}

		public void Dispose() {}

		static long NextSize(long size) {
			Require.OffsetMultiple("size", size, PageSize);
			// Azure doesn't charge us for the page storage anyway
			const long hundredMBs = 1024 * 1024 * 100;
			return size + hundredMBs;
		}
	}

}