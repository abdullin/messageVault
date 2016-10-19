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

	public sealed class CloudClient : IClient, IDisposable {
		readonly HttpClient _client;
		public readonly Uri Server;

		public static Func<Stream> StreamFactory = () => new MemoryStream();
		public string StreamPrefix { get; set; }

		public CloudClient(string url, string username, string password, string streamPrefix = null) {
			StreamPrefix = streamPrefix;
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
					var result = await _client.PostAsync("/streams/" + GetRealStreamName(stream), sc);
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

		public async Task<string> GetReaderSignatureAsync(string stream) {
			var result = await _client.GetAsync("/streams/" + GetRealStreamName(stream)).ConfigureAwait(false);
			result.EnsureSuccessStatusCode();
			var content = await result.Content.ReadAsStringAsync().ConfigureAwait(false);
			var response = JsonConvert.DeserializeObject<GetStreamResponse>(content);
			var signature = response.Signature;
			return signature;
		}

		public string GetReaderSignature(string stream) {
			var task = GetReaderSignatureAsync(stream);
			task.Wait();
			return task.Result;
		}
		
		public async Task<MessageReader> GetMessageReaderAsync(string stream) {
			var signature = await GetReaderSignatureAsync(stream).ConfigureAwait(false);
			return CloudSetup.GetReader(signature);
		}

		private string GetRealStreamName(string streamName) {
			return (StreamPrefix ?? "") + streamName;
		}

		public void Dispose() {
			_client.Dispose();
		}
	}
}