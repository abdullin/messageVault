using System.Threading;
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

		/// <summary>
		/// Gets the information about the current client.
		/// </summary>
		/// <param name="client">The client.</param>
		/// <returns>Task which returns current leader information, or <c>null</c> if master file doesn't exist and there's no master elected.</returns>
		public static async Task<LeaderInfo> Get(ICloudFactory client) {
			var blob = GetBlob(client);
			var exists = await blob.ExistsAsync();
			if (!exists) {
				return null;
			}
			var endpoint = blob.Metadata["endpoint"];
			return new LeaderInfo(endpoint);
		}

		/// <summary>
		/// Writes current node info to blob.
		/// </summary>
		/// <param name="storage">The storage.</param>
		/// <param name="token">The token.</param>
		/// <returns>Task representing writing process.</returns>
		/// <remarks>This is usually called when the current node becomes the master to notify everyone about it.</remarks>
		public async Task WriteToBlobAsync(ICloudFactory storage, CancellationToken token) {

			var blob = GetBlob(storage);

			var exists = await blob.ExistsAsync(token);
			if (!exists) {
				await blob.CreateAsync(0, AccessCondition.GenerateIfNoneMatchCondition("*"), null, null, token);
			}
			blob.Metadata["endpoint"] = this._endpoint;
			await blob.SetMetadataAsync(token);
		}

		static CloudPageBlob GetBlob(ICloudFactory cloudBlobClient) {
			var container = cloudBlobClient.GetSysContainerReference();

			var blob = container.GetPageBlobReference(Constants.MasterDataFileName);
			return blob;
		}
	}
}