using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace MessageVault.Server {

	public sealed class AppConfig {
		public  string PublicUri;
		public  string InternalUri;
		public ICloudFactory StorageAccount;

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



	public interface ICloudFactory {
		CloudBlobContainer GetContainerReference(string stream);
		CloudBlobContainer GetSysContainerReference();
		string GetSysPassword();
	}

	public sealed class SingleAccountFactory : ICloudFactory {
		readonly CloudBlobClient _storage;
		public SingleAccountFactory(CloudStorageAccount account) {
			_storage = account.CreateCloudBlobClient();
			_storage.DefaultRequestOptions.RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(0.5), 3);
			
		}

		public CloudBlobContainer GetContainerReference(string stream) {

			return _storage.GetContainerReference(stream);
			
		}

		public CloudBlobContainer GetSysContainerReference() {
			return _storage.GetContainerReference(Constants.SysContainer);
		}

		public string GetSysPassword() {
			return _storage.Credentials.ExportBase64EncodedKey();
		}
	}

}