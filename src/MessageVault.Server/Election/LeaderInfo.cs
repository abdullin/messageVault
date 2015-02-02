using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault.Server.Election {

	public sealed class LeaderInfo {

		readonly string _endpoint;
		
		public string GetEndpoint() {
			return _endpoint;
		}
		public LeaderInfo(string endpoint) {
			_endpoint = endpoint;
		}

		public static async Task<LeaderInfo> Get(ICloudFactory client) {
			var blob = GetBlob(client);
			var exists = await blob.ExistsAsync();
			if (!exists) {
				return null;
			}
			var endpoint = blob.Metadata["endpoint"];
			return new LeaderInfo(endpoint);
		}

		public async Task WriteToBlob(ICloudFactory storage) {

			var blob = GetBlob(storage);

			var exists = await blob.ExistsAsync();
			if (!exists) {
				blob.Create(0, AccessCondition.GenerateIfNoneMatchCondition("*"));
			}
			blob.Metadata["endpoint"] = this._endpoint;
			await blob.SetMetadataAsync();
		}

		static CloudPageBlob GetBlob(ICloudFactory cloudBlobClient) {
			var container = cloudBlobClient.GetSysContainerReference();

			var blob = container.GetPageBlobReference(Constants.MasterDataFileName);
			return blob;
		}
	}
}