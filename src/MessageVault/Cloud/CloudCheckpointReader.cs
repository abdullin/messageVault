using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault.Cloud {

	public sealed class CloudCheckpointReader {
		readonly CloudPageBlob _blob;

		public CloudCheckpointReader(CloudPageBlob blob) {
			_blob = blob;
		}

		public long Read() {
			// TODO: use etag and handle non-existent case
			_blob.FetchAttributes();
			return long.Parse(_blob.Metadata["position"]);
		}
	}

}