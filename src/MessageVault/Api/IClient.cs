using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MessageVault.Api {

	public interface IClient : IDisposable {
		Task<PostMessagesResponse> PostMessagesAsync(string stream, ICollection<Message> messages);
		Task<MessageReader> GetMessageReaderAsync(string stream);
	}

}