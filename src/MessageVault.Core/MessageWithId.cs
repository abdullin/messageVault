using System;
using System.IO;
using System.Text;

namespace MessageVault {

	public sealed class MessageWithId {

		public readonly MessageId Id;
		public readonly byte Attributes;
		public readonly byte[] Key;
		public readonly byte[] Value;
		public readonly uint Crc32;

		static readonly Encoding UTF8NoBOM = new UTF8Encoding(false);

		public string KeyAsString() {
			return UTF8NoBOM.GetString(Key);
		}

		public MessageWithId(MessageId id, byte attributes, byte[] key, byte[] value, uint crc32) {
			Attributes = attributes;
			Id = id;
			Key = key;
			Value = value;
			Crc32 = crc32;
		}

		public static MessageWithId Create(MessageId id, byte attributes, byte[] key, byte[] value) {
			var crc = attributes ^ Crc32Algorithm.Compute(key) ^ Crc32Algorithm.Compute(value) ^ (uint)id.GetHashCode();
			return new MessageWithId(id, attributes, key, value, crc);
		}
	}

}