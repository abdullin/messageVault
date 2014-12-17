using System;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault {

	public sealed class PositionWriter : IWriteableCheckpoint {
		readonly CloudPageBlob _blob;

		public PositionWriter(CloudPageBlob blob) {
			_blob = blob;
		}


		public long GetOrInitPosition() {
			if (!_blob.Exists()) {
				_blob.Metadata["position"] = "0";
				_blob.Create(512);
				return 0;
			}
			var position = _blob.Metadata["position"];
			var result = long.Parse(position);
			Ensure.ZeroOrGreater("position", result);
			return result;
		}

		public void Update(long position) {
			Require.ZeroOrGreater("position", position);
			_blob.Metadata["position"] = position.ToString();
			_blob.SetMetadata();
		}
	}

	public interface IWriteableCheckpoint {
		long GetOrInitPosition();
		void Update(long position);
	}


	public sealed class MemoryCheckpoint : IWriteableCheckpoint {
		long _value = 0;
		public long GetOrInitPosition() {
			return _value;
		}

		public void Update(long position) {
			Require.ZeroOrGreater("position", position);
			_value = position;
		}
	}


	public sealed class PositionReader {
		readonly CloudPageBlob _blob;

		public PositionReader(CloudPageBlob blob) {
			_blob = blob;
		}

		public long Read() {
			// TODO: use etag and handle non-existent case
			_blob.FetchAttributes();
			return long.Parse(_blob.Metadata["position"]);
		}
	}

}