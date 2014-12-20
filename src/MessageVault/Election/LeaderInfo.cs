using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault.Election {

	public sealed class LeaderInfo {

		readonly string _endpoint;
		
		public string GetEndpoint() {
			return _endpoint;
		}
		public LeaderInfo(string endpoint) {
			_endpoint = endpoint;
		}

		public static async Task<LeaderInfo> Get(CloudBlobClient client) {
			var blob = GetBlob(client);
			var exists = await blob.ExistsAsync();
			if (!exists) {
				return null;
			}
			var endpoint = blob.Metadata["endpoint"];
			return new LeaderInfo(endpoint);
		}

		public async Task WriteToBlob(CloudStorageAccount storage) {

			var blob = GetBlob(storage.CreateCloudBlobClient());

			var exists = await blob.ExistsAsync();
			if (!exists) {
				blob.Create(0, AccessCondition.GenerateIfNoneMatchCondition("*"));
			}
			blob.Metadata["endpoint"] = this._endpoint;
			await blob.SetMetadataAsync();
		}

		static CloudPageBlob GetBlob(CloudBlobClient cloudBlobClient) {
			var container = cloudBlobClient.GetContainerReference(Constants.LockContainer);

			var blob = container.GetPageBlobReference(Constants.MasterDataFileName);
			return blob;
		}
	}
}