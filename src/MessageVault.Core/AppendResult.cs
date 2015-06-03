using System.Collections.Generic;

namespace MessageVault {

	public sealed class AppendResult {
		public readonly IList<MessageId> Ids; 
		public readonly long Position;

		public AppendResult(IList<MessageId> ids, long position) {
			Ids = ids;
			Position = position;
		}
	}

}