using System;
using System.IO;

namespace MessageVault {

	public static class StorageFormat {
		public static void Write(BinaryWriter writer, MessageId id, Message item)
		{
			writer.Write(ReservedFormatVersion);
			writer.Write(item.Attributes);
			writer.Write(id.GetBytes());
			// we know for 100% that key length will be byte 
			writer.Write((byte)item.Key.Length);
			writer.Write(item.Key);
			// we know for 100% that value length will be ushort
			writer.Write((ushort)item.Value.Length);
			writer.Write(item.Value);
			writer.Write(item.Crc32);
		}

		public static int EstimateSize(Message item) {
			int sizeEstimate
				= 1 // magic byte 
				    + 1 // attributes byte
					+ 4 // CRC
					+ 16 // ID
					+ 1 + item.Key.Length // key
					+ 2 + item.Value.Length; // value
			return sizeEstimate;
		}

		public static MessageWithId Read(BinaryReader binary) {
			var version = binary.ReadByte();
			if (version != ReservedFormatVersion)
			{
				throw new InvalidOperationException("Unknown storage format");
			}
			var flags = binary.ReadByte();
			var id = binary.ReadBytes(16);
			var keyLength = binary.ReadByte();
			var key = binary.ReadBytes(keyLength);
			var len = binary.ReadUInt16();
			var data = binary.ReadBytes(len);
			var crc = binary.ReadUInt32();
			var uuid = new MessageId(id);
			var message = new MessageWithId(uuid, flags, key, data, crc);
			return message;
		}

		public const byte ReservedFormatVersion = 0x01;
	}

}