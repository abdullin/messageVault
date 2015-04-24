using System;

namespace MessageVault {
	
	public sealed class MessageToWrite {
		public readonly uint Flags;
		public readonly string Key;
		public readonly byte[] Value;

		public MessageToWrite(uint flags,  string key, byte[] value) {
			Flags = flags;
			Key = key;
			Value = value;
		}
	}

}