using System;
using System.Threading;
using System.Threading.Tasks;
using MessageVault.Api;

namespace MessageVault.Election {

	public sealed class LeaderInfoPoller {
		


		public async Task KeepPollingForLeaderInfo(CancellationToken token) {
			await Task.Delay(-1, token);
		}

		public async Task<Client> GetLeaderClientAsync() {
			throw new NotImplementedException();
		}
	}

}