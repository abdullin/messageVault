using System.Collections.Generic;

namespace MessageVault {

	/// <summary>
	///   A collection of loaded messages
	/// </summary>
	public sealed class MessageResult {
		public readonly IList<Message> Messages;

		public readonly long NextOffset;

		public MessageResult(IList<Message> messages, long nextOffset) {

			Require.Positive("nextOffset", nextOffset);
			Require.NotNull("messages", messages);

			Messages = messages;
			NextOffset = nextOffset;
		}

		public static MessageResult Empty(long offset) {
			Require.ZeroOrGreater("offset", offset);
			return new MessageResult(new Message[0], offset);
		}

		public bool HasMessages() {
			return Messages.Count > 0;
		}
	}

}