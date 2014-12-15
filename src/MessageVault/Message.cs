using System.Collections.Generic;
using System.IO;
using System.Text;

namespace MessageVault {

	public sealed class Message {
		public readonly string Contract;
		public readonly byte[] Data;

		public Message(string contract, byte[] data) {
			Contract = contract;
			Data = data;
		}
	}

	public static class MessageFramer {
		public static void WriteMessages(ICollection<Message> messages, Stream stream) {
			using (var bin = new BinaryWriter(stream, Encoding.UTF8, true)) {
				// int
				bin.Write(messages.Count);

				foreach (var message in messages) {
					bin.Write(message.Contract);
					bin.Write(message.Data.Length); //int32
					bin.Write(message.Data);
				}
			}
		}

		public static ICollection<Message> ReadMessages(Stream source) {
			using (var bin = new BinaryReader(source, Encoding.UTF8, true)) {
				// TODO: use a buffer pool
				var len = bin.ReadInt32();
				var result = new List<Message>(len);
				for (int i = 0; i < len; i++) {
					var contract = bin.ReadString();
					var size = bin.ReadInt32();
					var data = bin.ReadBytes(size);
					result.Add(new Message(contract, data));
					
				}
				return result;
			}
			
		}
	}

}