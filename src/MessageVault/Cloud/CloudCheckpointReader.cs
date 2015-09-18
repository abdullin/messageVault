using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault.Cloud {

	public sealed class CloudCheckpointReader : ICheckpointReader{
		readonly CloudPageBlob _blob;

		public CloudCheckpointReader(CloudPageBlob blob) {
			Require.NotNull("blob", blob);
			_blob = blob;
		}

		public long Read() {
			try {
				// blob exists will actually fetch attributes but suppress error on 404
				if (!_blob.Exists()) {
					return 0;
				}
				var s = _blob.Metadata[CloudSetup.CheckpointMetadataName];
				var result = long.Parse(s);
				Ensure.ZeroOrGreater("result", result);
				return result;
			}
			catch (StorageException ex) {
				// if forbidden, then we might have an expired SAS token
				if (ex.RequestInformation != null && ex.RequestInformation.HttpStatusCode == 403) {
					throw new ForbiddenException("Can't read blob", ex);
				}
				throw;

			}
		}

	    public void Dispose() {
	        
	    }
	}

}