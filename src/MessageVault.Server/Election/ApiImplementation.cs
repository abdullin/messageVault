using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MessageVault.Api;
using MessageVault.Cloud;
using MessageVault.Server.Auth;

using Serilog;
using StatsdClient;

namespace MessageVault.Server.Election {

	public sealed class ApiImplementation {
		MessageWriteScheduler _scheduler;
		readonly ICloudFactory _client;
		readonly LeaderInfoPoller _poller;

		public static ApiImplementation Create(ICloudFactory account, LeaderInfoPoller poller, AuthData auth) {
			return new ApiImplementation(account, poller);
		}

		ApiImplementation(ICloudFactory client, LeaderInfoPoller poller) {
			_client = client;
			_poller = poller;
		}

		public void EnableDirectWrites(MessageWriteScheduler scheduler) {
			_scheduler = scheduler;
			Log.Verbose("API will handle writes on this node");
		}

		public void DisableDirectWrites() {
			if (_scheduler != null) {
				_scheduler = null;
				Log.Verbose("API will forward writes to leader");	
			}
		}

		public GetStreamResponse GetReadAccess(string stream) {
			using (Metrics.StartTimer("api.read")) {
				var container = _client.GetContainerReference(stream);
				var signature = CloudSetup.GetReadAccessSignature(container);
				return new GetStreamResponse {
					Signature = signature
				};
			}
		}

		public async Task<PostMessagesResponse> Append(string id, ICollection<Message> writes) {
			var writer = _scheduler;
			if (null != writer) {
				using (Metrics.StartTimer("api.append")) {
					var result = await writer.Append(id, writes);
					return PostMessagesResponse.FromAppendResult(result);
				}
			}
			using (Metrics.StartTimer("api.forward")) {
				var endpoint = await _poller.GetLeaderClientAsync();
				var result = await endpoint.PostMessagesAsync(id, writes);
				return result;
			}
		}
	}

}