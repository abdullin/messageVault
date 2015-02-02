using System;
using System.Collections.Generic;
using MessageVault.Cloud;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault.Server {

	public sealed class AppConfig {
		public string PublicUri;
		public string InternalUri;
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
			_storage.DefaultRequestOptions.RetryPolicy = CloudSetup.RetryPolicy;
		}

		public CloudBlobContainer GetContainerReference(string stream) {
			var containerName = Constants.DataContainerPrefix + stream;
			return _storage.GetContainerReference(containerName);
		}

		public CloudBlobContainer GetSysContainerReference() {

			return _storage.GetContainerReference(Constants.SysContainer);
		}

		public string GetSysPassword() {
			return _storage.Credentials.ExportBase64EncodedKey();
		}
	}

	public sealed class MultiAccountFactory : ICloudFactory {
		readonly CloudBlobClient _system;
		readonly IReadOnlyDictionary<string, CloudBlobClient> _streams;

		public MultiAccountFactory(CloudStorageAccount system,
			Dictionary<string, CloudStorageAccount> streams) {
			var store = new Dictionary<string, CloudBlobClient>(StringComparer.InvariantCultureIgnoreCase);
			_system = CreateClient(system);
			foreach (var account in streams) {
				store.Add(account.Key, CreateClient(account.Value));
			}
			_streams = store;
		}

		static CloudBlobClient CreateClient(CloudStorageAccount system) {
			var client = system.CreateCloudBlobClient();
			client.DefaultRequestOptions.RetryPolicy = CloudSetup.RetryPolicy;
			return client;
		}

		public CloudBlobContainer GetContainerReference(string stream) {
			var containerName = Constants.DataContainerPrefix + stream;
			CloudBlobClient value;
			if (_streams.TryGetValue(stream, out value)) {
				return value.GetContainerReference(containerName);
			}
			return _system.GetContainerReference(containerName);
		}

		public CloudBlobContainer GetSysContainerReference() {
			return _system.GetContainerReference(Constants.SysContainer);
		}

		public string GetSysPassword() {
			return _system.Credentials.ExportBase64EncodedKey();
		}
	}

}