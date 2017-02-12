using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault.Cloud {

	public sealed class CloudPageReader : IPageReader {
		readonly CloudPageBlob _blob;

		public CloudPageReader(CloudPageBlob blob) {

			
			Require.NotNull("blob", blob);
			_blob = blob;
		}

		public void DownloadRangeToStream(Stream stream, long offset, int length) {
			Require.NotNull("stream", stream);
			Require.ZeroOrGreater("offset", offset);
			Require.Positive("length", length);

			try {
				var context = new OperationContext();
				context.SendingRequest += (sender, e) => {
					e.Request.Headers["if-match"] = "*";
				};
				_blob.DownloadRangeToStream(stream, offset, length, null, null, context);
			}
			catch (StorageException ex) {
				// if forbidden, then we might have an expired SAS token
				if (ex.RequestInformation != null && ex.RequestInformation.HttpStatusCode == 403) {
					throw new ForbiddenException("Can't read blob", ex);
				}
				throw;
			}
		}


		public async Task DownloadRangeToStreamAsync(Stream stream, long offset, int length)
		{
			Require.NotNull("stream", stream);
			Require.ZeroOrGreater("offset", offset);
			Require.Positive("length", length);

			try
			{
				var context = new OperationContext();
				context.SendingRequest += (sender, e) => {
					e.Request.Headers["if-match"] = "*";
				};
				await _blob.DownloadRangeToStreamAsync(stream, offset, length, null, null, context).ConfigureAwait(false);
			}
			catch (StorageException ex)
			{
				// if forbidden, then we might have an expired SAS token
				if (ex.RequestInformation != null && ex.RequestInformation.HttpStatusCode == 403)
				{
					throw new ForbiddenException("Can't read blob", ex);
				}
				throw;
			}
		}

		public void Dispose() {}
	}

}