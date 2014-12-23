using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault.Cloud {

	public sealed class CloudCheckpointReader : ICheckpointReader{
		readonly CloudPageBlob _blob;

		public CloudCheckpointReader(CloudPageBlob blob) {
			Require.NotNull("blob", blob);
			_blob = blob;
		}

		public long Read() {
			// blob exists will actually fetch attributes but suppress error on 404
			if (!_blob.Exists()) {
				return 0;
			}
			var s = _blob.Metadata[CloudSetup.CheckpointMetadataName];
			var result = long.Parse(s);
			Ensure.ZeroOrGreater("result", result);
			return result;
		}
	}

}