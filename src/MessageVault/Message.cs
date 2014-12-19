using System;
using System.IO;

namespace MessageVault {

	public sealed class Message {
		public readonly MessageId Id;
		public readonly string Key;
		public readonly byte[] Value;

		public Message(MessageId id, string key, byte[] value) {
			Id = id;
			Key = key;
			Value = value;
		}
	}

}