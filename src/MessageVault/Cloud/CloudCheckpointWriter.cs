using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault.Cloud {

	public sealed class CloudCheckpointWriter : ICheckpointWriter {
		readonly CloudPageBlob _blob;
		string _etag;

		public CloudCheckpointWriter(CloudPageBlob blob) {
			_blob = blob;
		}


		public long GetOrInitPosition() {
			if (!_blob.Exists()) {
				_blob.Metadata["position"] = "0";
				_blob.Create(512, AccessCondition.GenerateIfNoneMatchCondition("*"));
				_etag = _blob.Properties.ETag;
				return 0;
			}
			var position = _blob.Metadata["position"];
			_etag = _blob.Properties.ETag;
			var result = long.Parse(position);
			Ensure.ZeroOrGreater("position", result);
			return result;
		}

		public void Update(long position) {
			Require.ZeroOrGreater("position", position);
			_blob.Metadata["position"] = position.ToString();
			_blob.SetMetadata(AccessCondition.GenerateIfMatchCondition(_etag));
			_etag = _blob.Properties.ETag;
		}
	}

}