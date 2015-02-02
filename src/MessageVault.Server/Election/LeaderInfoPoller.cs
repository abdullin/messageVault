using System.Threading;
using System.Threading.Tasks;
using MessageVault.Api;
using Microsoft.WindowsAzure.Storage;
using Serilog;

namespace MessageVault.Server.Election {

	public sealed class LeaderInfoPoller {
		public LeaderInfoPoller(ICloudFactory storage) {
			_storage = storage;
		}

		readonly ICloudFactory _storage;
		Client _client;
		string _endpoint;

		public async Task KeepPollingForLeaderInfo(CancellationToken token) {
			while (!token.IsCancellationRequested) {
				try {
					var info = await LeaderInfo.Get(_storage);
					if (info == null) {
						_client = null;
						await Task.Delay(500, token);
						continue;
					}
					var newEndpoint = info.GetEndpoint();
					if (_endpoint != newEndpoint) {
						Log.Information("Detected new leader {endpoint}", newEndpoint);
						_endpoint = newEndpoint;
						var password = _storage.GetSysPassword();
						_client = new Client(_endpoint, Constants.ClusterNodeUser, password);
					}

					await Task.Delay(3500, token);
				}
				catch (StorageException ex) {
					Log.Warning(ex, "Failed to refresh leader info");
					token.WaitHandle.WaitOne(1000);
				}
			}
		}

		public async Task<Client> GetLeaderClientAsync() {
			while (_client == null) {
				await Task.Delay(100);
			}
			return _client;
		}
	}

}