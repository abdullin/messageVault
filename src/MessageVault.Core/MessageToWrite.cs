using System;

namespace MessageVault {
	[Flags]
	public enum MessageFlags : int {
		None,
		LZ4_32 = 0x01,
		LZ4_32HC = 0x02,
	}

	public sealed class MessageToWrite {
		public readonly MessageFlags Flags;
		public readonly string Key;
		public readonly byte[] Value;

		public MessageToWrite(MessageFlags flags,  string key, byte[] value) {
			Key = key;
			Value = value;
		}
	}

}