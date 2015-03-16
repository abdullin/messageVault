using System.Collections.Generic;
using System.IO;
using System.Text;

namespace MessageVault.Api {

	/// <summary>
	/// Handles messages formatting to be passed in a stream (such as HTTP request body).
	/// </summary>
	public static class ApiMessageFramer {

		public static void WriteMessages(ICollection<MessageToWrite> messages, Stream stream) {
			using (var bin = new BinaryWriter(stream, Encoding.UTF8, true)) {
				// int
				bin.Write(messages.Count);

				foreach (var message in messages) {
					bin.Write(message.Key);
					bin.Write(message.Value.Length); //int32
					bin.Write(message.Value);
				}
			}
		}

		public static ICollection<MessageToWrite> ReadMessages(Stream source) {
			using (var bin = new BinaryReader(source, Encoding.UTF8, true)) {
				// TODO: use a buffer pool
				var len = bin.ReadInt32();
				var result = new List<MessageToWrite>(len);
				for (int i = 0; i < len; i++) {
					var contract = bin.ReadString();
					var size = bin.ReadInt32();
					var data = bin.ReadBytes(size);
					result.Add(new MessageToWrite(contract, data));
				}
				return result;
			}
		}
	}
}