using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MessageVault.Cloud;
using MessageVault.MemoryPool;

namespace MessageVault.Api {

	public interface IClient : IDisposable {
		Task<PostMessagesResponse> PostMessagesAsync(string stream, ICollection<Message> messages);
		Task<MessageReader> GetMessageReaderAsync(string stream);
		MessageReader GetMessageReader(string stream);
		MessageFetcher GetFetcher(string stream, IMemoryStreamManager manager);
	}


	public sealed class UnpackedMessage {
		public readonly byte[] Key;
		public readonly byte[] Value;

		public UnpackedMessage(byte[] key, byte[] value) {
			Key = key;
			Value = value;
		}
	}


}