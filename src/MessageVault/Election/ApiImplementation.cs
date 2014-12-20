using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MessageVault.Api;
using Serilog;

namespace MessageVault.Election {

	public sealed class ApiImplementation {
		MessageWriteScheduler _scheduler;

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
			throw new NotImplementedException();
		}

		public async Task<PostMessagesResponse> Append(string id, ICollection<MessageToWrite> writes) {
			throw new NotImplementedException();
		}
	}

}