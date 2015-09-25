using System;
using System.Collections.Generic;

namespace MessageVault.Api {

	public sealed class PostMessagesResponse {
		public long Position { get; set; }
		public IList<long> Offsets { get; set; }

		public static PostMessagesResponse FromAppendResult(AppendResult r) {
			var array = new long[r.Ids.Count];

			for (int i = 0; i < array.Length; i++) {
				array[i] = r.Ids[i].GetOffset();
			}
			return new PostMessagesResponse() {
				Position = r.Position,
				Offsets = array
			};
		}
	}
}