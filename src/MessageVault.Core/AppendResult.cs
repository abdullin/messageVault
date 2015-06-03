using System.Collections.Generic;

namespace MessageVault {

	public sealed class AppendResult {
		public readonly ICollection<MessageId> Ids; 
		public readonly long Position;

		public AppendResult(ICollection<MessageId> ids, long position) {
			Ids = ids;
			Position = position;
		}
	}

}