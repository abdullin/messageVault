using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MessageVault.Api {

	public interface IClient : IDisposable {
		Task<PostMessagesResponse> PostMessagesAsync(string stream, ICollection<Message> messages);
		Task<MessageReader> GetMessageReaderAsync(string stream);
		MessageReader GetMessageReader(string stream);
	}


	public sealed class UnpackedMessage {
		public readonly byte[] Key;
		public readonly byte[] Value;

		public UnpackedMessage(byte[] key, byte[] value) {
			Key = key;
			Value = value;
		}
	}

	[Flags]
	public enum MessageFlags : byte {
		None = 0,
		LZ4 = 0x01,
		ToBeContinued = 0x02,
	}

}