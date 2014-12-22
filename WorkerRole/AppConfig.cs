using System;
using Microsoft.WindowsAzure.Storage;

namespace WorkerRole {

	public sealed class AppConfig {
		public  string PublicUri;
		public  string InternalUri;
		public CloudStorageAccount StorageAccount;

		public void ThrowIfInvalid() {
			if (string.IsNullOrWhiteSpace(PublicUri)) {
				throw new InvalidOperationException("Specify public uri");
			}
			if (string.IsNullOrWhiteSpace(InternalUri)) {
				throw new InvalidOperationException("Specify private uri");
			}
			if (StorageAccount == null) {
				throw new InvalidOperationException("Storage account must be specified");
			}
		}
	}

}