using System.IO;

namespace MessageVault.Api {

	public static class CacheStorage {
		const ushort FooterSignature = 0xC0DE;
		const ushort HeaderSignature = 0xBAD1;


		public static MessageWithId Read(BinaryReader binary)
		{
			var header = binary.ReadUInt16();
			if (header == 0)
			{
				throw new NoDataException();
			}
			if (header != HeaderSignature)
			{
				throw new InvalidStorageFormatException("Unknown storage format :" + header);
			}
			var id = binary.ReadBytes(16);
			var keyLength = binary.ReadByte();
			var key = binary.ReadBytes(keyLength);
			var dataLength = binary.ReadUInt16();
			var data = binary.ReadBytes(dataLength);
			var footer = binary.ReadUInt16();

			if (footer != FooterSignature) {
				throw new InvalidStorageFormatException("Unknown footer format: " + footer);
			}
			
			var uuid = new MessageId(id);
			return new MessageWithId(uuid, 0, key, data, 0);
		}

		public static void Write(BinaryWriter writer, MessageWithId item) {
			writer.Write(HeaderSignature);

			writer.Write(item.Id.GetBytes());
			// we know for 100% that key length will be byte 

			writer.Write((byte)item.Key.Length);
			writer.Write(item.Key);
			// we know for 100% that value length will be ushort
			writer.Write((ushort)item.Value.Length);
			writer.Write(item.Value);
			writer.Write(FooterSignature);
		}
	}

}