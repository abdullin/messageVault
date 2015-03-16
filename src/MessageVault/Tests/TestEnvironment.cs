using System;
using System.Threading;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault.Tests {

	public static class TestEnvironment {
		static TestEnvironment() {
			Account = CloudStorageAccount.DevelopmentStorageAccount;
			Client = Account.CreateCloudBlobClient();
		}

		static readonly CloudStorageAccount Account;
		public static readonly CloudBlobClient Client;

		static int _sequence;

		public static string GetContainerName(string prefix) {
			
			var value = Interlocked.Increment(ref _sequence);
			var container = string.Format("{0}-{1:yyyy-MM-dd-hh-mm-ss}-{2}",
				prefix.ToLowerInvariant(),
				DateTime.Now,
				value);
			return container;
		}
	}
}
