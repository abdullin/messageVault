using System.IO;
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

			_blob.DownloadRangeToStream(stream, offset, length);
		}

	    public void Dispose() {
	        
	    }
	}

}