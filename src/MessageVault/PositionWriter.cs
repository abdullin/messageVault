using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault {

	public sealed class PositionWriter {
		readonly CloudPageBlob _blob;

		public PositionWriter(CloudPageBlob blob) {
			_blob = blob;
		}

		
		public long GetOrInitPosition() {
			if (!_blob.Exists())
			{				
				_blob.Metadata["position"] = "0";
				_blob.Create(512);
				return 0;
			}
			string position = _blob.Metadata["position"];
			return long.Parse(position);
		}

		public void Update(long position) {
			_blob.Metadata["position"] = position.ToString();
			_blob.SetMetadata();
		}
	}


}