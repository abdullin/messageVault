using System.Collections.Generic;
using System.Threading.Tasks;
using MessageVault.Api;
using MessageVault.Cloud;
using MessageVault.Server.Auth;
using Serilog;
using StatsdClient;

namespace MessageVault.Server.Election {
	/// <summary>
	/// API service used to apped messages to the message stream.
	/// </summary>
	public sealed class ApiImplementation {
		MessageWriteScheduler _writeScheduler;
		readonly ICloudFactory _client;
		readonly LeaderInfoPoller _leader;

		public static ApiImplementation Create(ICloudFactory account, LeaderInfoPoller leader, AuthData auth) {
			return new ApiImplementation(account, leader);
		}

		ApiImplementation(ICloudFactory client, LeaderInfoPoller leader) {
			_client = client;
			_leader = leader;
		}

		public void EnableDirectWrites(MessageWriteScheduler scheduler) {
			_writeScheduler = scheduler;
			Log.Verbose("API will handle writes on this node");
		}

		public void DisableDirectWrites() {
			if (_writeScheduler != null) {
				_writeScheduler = null;
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

		public async Task<PostMessagesResponse> Append(string id, ICollection<MessageToWrite> writes) {
			var writer = _writeScheduler;
			if (null != writer) {
				using (Metrics.StartTimer("api.append")) {
					var result = await writer.Append(id, writes);
					return new PostMessagesResponse {
						Position = result
					};
				}
			}
			using (Metrics.StartTimer("api.forward")) {
				var endpoint = await _leader.GetLeaderClientAsync();
				var result = await endpoint.PostMessagesAsync(id, writes);
				return result;
			}
		}
	}

}