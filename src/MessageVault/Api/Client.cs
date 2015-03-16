using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using MessageVault.Cloud;
using Newtonsoft.Json;

namespace MessageVault.Api {

	public sealed class Client : IDisposable {
		readonly HttpClient _client;

		public Client(string url, string username, string password) {
			_client = new HttpClient {
				BaseAddress = new Uri(url)
			};

			SetupBasicAuth(username, password);
		}

		void SetupBasicAuth(string username, string password) {
			var byteArray = Encoding.ASCII.GetBytes(username + ":" + password);
			_client.DefaultRequestHeaders.Authorization =
				new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
		}

		public async Task<PostMessagesResponse> PostMessagesAsync(string stream, ICollection<MessageToWrite> messages) {
			// TODO: use a buffer pool
			using (var mem = new MemoryStream()) {

				ApiMessageFramer.WriteMessages(messages, mem);
				mem.Seek(0, SeekOrigin.Begin);

				using (var sc = new StreamContent(mem)) {
					var result = await _client.PostAsync("/streams/" + stream, sc);
					result.EnsureSuccessStatusCode();
					var content = await result.Content.ReadAsStringAsync();
					return JsonConvert.DeserializeObject<PostMessagesResponse>(content);
				}
			}
		}
		
		public async Task<MessageReader> GetMessageReaderAsync(string stream) {
			var result = await _client.GetAsync("/streams/" + stream);
			result.EnsureSuccessStatusCode();
			var content = await result.Content.ReadAsStringAsync();
			var response = JsonConvert.DeserializeObject<GetStreamResponse>(content);
			
			return CloudSetup.GetReader(response.Signature);
		}

		public void Dispose() {
			_client.Dispose();
		}
	}
}