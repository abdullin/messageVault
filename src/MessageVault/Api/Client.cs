using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using MessageVault.Cloud;
using Newtonsoft.Json;

namespace MessageVault.Api {

	public sealed class Client : IClient, IDisposable {
		readonly HttpClient _client;
		public readonly Uri Server;

		public static Func<Stream> StreamFactory = () => new MemoryStream(); 

		public Client(string url, string username, string password) {
			Server = new Uri(url);
			_client = new HttpClient {
				BaseAddress = Server
			};
			SetupBasicAuth(username, password);
		}

		void SetupBasicAuth(string username, string password) {
			var byteArray = Encoding.ASCII.GetBytes(username + ":" + password);
			_client.DefaultRequestHeaders.Authorization =
				new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
		}

		public async Task<PostMessagesResponse> PostMessagesAsync(string stream, ICollection<Message> messages) {
			
			using (var mem = StreamFactory()) {

				TransferFormat.WriteMessages(messages, mem);

				mem.Seek(0, SeekOrigin.Begin);
				
				using (var sc = new StreamContent(mem)) {
					var result = await _client.PostAsync("/streams/" + stream, sc);
					result.EnsureSuccessStatusCode();
					var content = await result.Content.ReadAsStringAsync();
					return JsonConvert.DeserializeObject<PostMessagesResponse>(content);
				}
			}
		}

		public MessageReader GetMessageReader(string stream) {
			var task = GetMessageReaderAsync(stream);
			task.Wait();
			return task.Result;
		}

		public async Task<string> GetReaderSignature(string stream) {
			var result = await _client.GetAsync("/streams/" + stream);
			result.EnsureSuccessStatusCode();
			var content = await result.Content.ReadAsStringAsync();
			var response = JsonConvert.DeserializeObject<GetStreamResponse>(content);
			var signature = response.Signature;
			return signature;
		}
		
		public async Task<MessageReader> GetMessageReaderAsync(string stream) {
			var signature = await GetReaderSignature(stream);
			return CloudSetup.GetReader(signature);
		}

		public void Dispose() {
			_client.Dispose();
		}
	}
}