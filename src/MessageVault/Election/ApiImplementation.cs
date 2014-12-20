using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MessageVault.Api;
using MessageVault.Cloud;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Serilog;

namespace MessageVault.Election {

	public sealed class ApiImplementation {
		MessageWriteScheduler _scheduler;
		readonly CloudBlobClient _client;

		public static ApiImplementation Create(CloudStorageAccount account) {
			return new ApiImplementation(account.CreateCloudBlobClient());
		}

		ApiImplementation(CloudBlobClient	 client) {
			_client = client;
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
			var signature = CloudSetup.GetReadAccessSignature(_client, stream);
			return new GetStreamResponse {
				Signature = signature
			};
		}

		public async Task<PostMessagesResponse> Append(string id, ICollection<MessageToWrite> writes) {
			var writer = _scheduler;
			if (null != writer) {
				var result = await writer.Append(id, writes);
				return new PostMessagesResponse() {
					Position = result
				};
			}
			throw new NotImplementedException();
		}
	}

}