using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using MessageVault.Cloud;
using Newtonsoft.Json;

namespace MessageVault.Api {

	public sealed class Client : IDisposable {
		readonly HttpClient _client;


		public Client(string url) {
			_client = new HttpClient {
				BaseAddress = new Uri(url)
			};
		}

		

		public async Task<PostMessagesResponse> PostMessagesAsync(string stream, ICollection<MessageToWrite> messages) {
			// TODO: use a buffer pool
			using (var mem = new MemoryStream()) {

				ApiMessageFramer.WriteMessages(messages, mem);
				mem.Seek(0, SeekOrigin.Begin);

				using (var sc = new StreamContent(mem)) {
					
					var result = await _client.PostAsync("/streams/" + stream, sc);
					
					var content = await result.Content.ReadAsStringAsync();

					return JsonConvert.DeserializeObject<PostMessagesResponse>(content);
				}
			}
		}

		
		public async Task<MessageReader> GetMessageReaderAsync(string stream) {

			var result = await _client.GetAsync("/streams/" + stream);
			var content = await result.Content.ReadAsStringAsync();
			var response = JsonConvert.DeserializeObject<GetStreamResponse>(content);
			// TODO: handle error
			//Console.WriteLine(response.Signature);
			return CloudSetup.GetReader(response.Signature);
		}


		public void Dispose() {
			_client.Dispose();
		}
	}

}