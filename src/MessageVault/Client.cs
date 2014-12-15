using System;
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

		

		public async Task<string> PostMessageAsync(string stream, byte[] message) {
			using (var mem = new MemoryStream(message)) {
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