using System;
using System.IO;
using LZ4s;

namespace MessageVault {

	public static class MessageFormat {
		public static void Write(BinaryWriter writer, MessageId id, MessageToWrite item)
		{
			writer.Write(ReservedFormatVersion);
			writer.Write((int)item.Flags);
			writer.Write(id.GetBytes());
			writer.Write(item.Key);
			
			writer.Write(item.Value.Length);
			writer.Write(item.Value);
			
		}

		public static int EstimateSize(MessageToWrite item) {
			int sizeEstimate
				= 1 // magic byte 
				    + 4 // flags INT32
					+ 16 // ID
					+ 4 + 2 * item.Key.Length // key
					+ 4 + item.Value.Length; // value
			return sizeEstimate;
		}

		public static Message Read(BinaryReader binary) {
			var version = binary.ReadByte();
			if (version != ReservedFormatVersion)
			{
				throw new InvalidOperationException("Unknown storage format");
			}
			var flags = binary.ReadInt32();
			var id = binary.ReadBytes(16);
			var contract = binary.ReadString();
			var len = binary.ReadInt32();
			var data = binary.ReadBytes(len);
			var uuid = new MessageId(id);
			var message = new Message(uuid, (MessageFlags) flags, contract, data);
			return message;
		}

		public const byte ReservedFormatVersion = 0x01;
	}

}