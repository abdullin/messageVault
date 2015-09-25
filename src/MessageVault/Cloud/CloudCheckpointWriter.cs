using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault.Cloud {

	public sealed class CloudCheckpointWriter : ICheckpointWriter {
		readonly CloudPageBlob _blob;
		string _etag;

		public CloudCheckpointWriter(CloudPageBlob blob) {
			Require.NotNull("blob", blob);
			_blob = blob;
		}


		public long GetOrInitPosition() {
			// blob.Exists actually fetches metadata
			if (!_blob.Exists()) {
				_blob.Metadata[CloudSetup.CheckpointMetadataName] = "0";
				_blob.Create(0, AccessCondition.GenerateIfNoneMatchCondition("*"));
				_etag = _blob.Properties.ETag;
				return 0;
			}
			var position = _blob.Metadata[CloudSetup.CheckpointMetadataName];
			_etag = _blob.Properties.ETag;
			var result = long.Parse(position);
			Ensure.ZeroOrGreater("position", result);
			return result;
		}

		public void Update(long position) {
			Require.ZeroOrGreater("position", position);

			Throw.OnEtagMismatchDuringAppend(() => {
				_blob.Metadata[CloudSetup.CheckpointMetadataName] = Convert.ToString(position);
				_blob.SetMetadata(AccessCondition.GenerateIfMatchCondition(_etag));
				_etag = _blob.Properties.ETag;
			});
		}

		public void Dispose() {}
	}

}