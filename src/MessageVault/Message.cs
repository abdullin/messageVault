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

		public static Message Read(BinaryReader binary) {
			var version = binary.ReadByte();
			if (version != Constants.ReservedFormatVersion)
			{
				throw new InvalidOperationException("Unknown storage format");
			}
			var id = binary.ReadBytes(16);
			var contract = binary.ReadString();
			var len = binary.ReadInt32();
			var data = binary.ReadBytes(len);
			var uuid = new MessageId(id);
			var message = new Message(uuid, contract, data);
			return message;
		}
	}

}