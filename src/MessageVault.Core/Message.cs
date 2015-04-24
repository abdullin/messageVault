using System;
using System.IO;

namespace MessageVault {

	public sealed class Message {
		public readonly MessageId Id;
		public readonly uint Flags;
		public readonly string Key;
		public readonly byte[] Value;

		public Message(MessageId id, uint flags, string key, byte[] value) {
			Flags = flags;
			Id = id;
			Key = key;
			Value = value;
		}
	}

}