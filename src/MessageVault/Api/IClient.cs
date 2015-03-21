using System.Collections.Generic;
using System.Threading.Tasks;

namespace MessageVault.Api {

	public interface IClient {
		Task<PostMessagesResponse> PostMessagesAsync(string stream, ICollection<MessageToWrite> messages);
		Task<MessageReader> GetMessageReaderAsync(string stream);
	}

}