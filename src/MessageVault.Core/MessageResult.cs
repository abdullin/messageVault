using System.Collections.Generic;

namespace MessageVault {

	/// <summary>
	///   A collection of loaded messages
	/// </summary>
	public sealed class MessageResult {
		public readonly IList<MessageWithId> Messages;

		public readonly long NextOffset;

		public MessageResult(IList<MessageWithId> messages, long nextOffset) {

			Require.Positive("nextOffset", nextOffset);
			Require.NotNull("messages", messages);

			Messages = messages;
			NextOffset = nextOffset;
		}

		public static MessageResult Empty(long offset) {
			Require.ZeroOrGreater("offset", offset);
			return new MessageResult(new MessageWithId[0], offset);
		}

		public bool HasMessages() {
			return Messages.Count > 0;
		}
	}

}