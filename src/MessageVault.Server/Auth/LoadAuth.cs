using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Serilog;

namespace MessageVault.Server.Auth {

	public static class LoadAuth {


		static AuthData GetEmptyConfig() {
			var log = Log.ForContext<AuthData>();
			
			log.Warning(
				"Auth {blob} doesn't exist in {container}. Using default login/password", 
				Constants.SysContainer, 
				Constants.AuthFileName
				);
			
			return AuthData.Default();
		}

		public static AuthData LoadFromStorageAccount(CloudStorageAccount account) {

			
			var client = account.CreateCloudBlobClient();
			client.DefaultRequestOptions.RetryPolicy = new LinearRetry(TimeSpan.FromMilliseconds(500), 3);
			var container = client.GetContainerReference(Constants.SysContainer);
			var blob = container.GetBlockBlobReference(Constants.AuthFileName);
			if (!blob.Exists()) {
				return GetEmptyConfig();
			}
			var source = blob.DownloadText();
			return AuthData.Deserialize(source);
		}

	}

}