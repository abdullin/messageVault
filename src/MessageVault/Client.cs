using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;

namespace MessageVault {

	public sealed class Client : IDisposable {
		readonly HttpClient _client;


		public Client(string url) {
			_client = new HttpClient {
				BaseAddress = new Uri(url)
			};
		}

		

		public async Task<string> PostMessagesAsync(string stream, ICollection<Message> messages) {
			// TODO: start using a buffer pool


			using (var mem = new MemoryStream()) {

				MessageFramer.WriteMessages(messages, mem);
				mem.Seek(0, SeekOrigin.Begin);

				using (var sc = new StreamContent(mem)) {
					
					var result = await _client.PostAsync("/streams/" + stream, sc);
					Console.WriteLine("got result");
					var content = await result.Content.ReadAsStringAsync();
					Console.WriteLine("content");
					return content;
				}
			}
		}



		public void Dispose() {
			_client.Dispose();
		}
	}

}