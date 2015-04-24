using System;
using System.Text;

namespace MessageVault {
	
	public sealed class Message {

		public static readonly Encoding UTF8NoBOM = new UTF8Encoding(false);

		public readonly byte Attributes;
		public readonly byte[] Key;
		public readonly byte[] Value;
		public readonly uint Crc32;

		public Message(byte attributes, byte[] key, byte[] value, uint crc32 ) {
			if (key.Length > Constants.MaxKeySize) {
				throw new ArgumentException("key can't be larger than " + Constants.MaxKeySize, "key");
			}
			if (value.Length > Constants.MaxValueSize) {
				throw new ArgumentException("value can't be larger than " + Constants.MaxKeySize, "value");
			}
			Attributes = attributes;
			Key = key;
			Value = value;
			Crc32 = crc32;
		}

		public static Message Create(byte[] key, byte[] value, byte attributes = 0) {
			var crc = attributes ^ Crc32Algorithm.Compute(key) ^ Crc32Algorithm.Compute(value);
			return new Message(attributes, key, value, crc);
		}

		public static Message Create(string key, byte[] value, byte attributes = 0) {
			var bytes = UTF8NoBOM.GetBytes(key);
			return Create(bytes, value, attributes);
		}

	}

}