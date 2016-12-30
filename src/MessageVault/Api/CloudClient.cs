using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security;
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


		static readonly byte[] Entropy = new Guid("c9d047c8-f7d0-4b5d-a207-6b551b8d05a6").ToByteArray();

		/// <summary>
		/// Encrypt credentials using data protection API (with user scope)
		/// </summary>
		/// <param name="user"></param>
		/// <param name="password"></param>
		/// <returns></returns>
		public static string EncryptCredentuals(string user, string password) {
			var binary = CredentialsToHeader(user, password);
			var data= ProtectedData.Protect(
				binary,
				Entropy,
				DataProtectionScope.CurrentUser);
			return Convert.ToBase64String(data);
		}

		/// <summary>
		/// Creates a new cloud client using credentials that were previously created with a call to <see cref="EncryptCredentuals"/>.
		/// </summary>
		/// <param name="url"></param>
		/// <param name="encrypted"></param>
		/// <param name="streamPrefix"></param>
		public CloudClient(string url, string encrypted, string streamPrefix = null) {
			StreamPrefix = streamPrefix;
			Server = new Uri(url);
			_client = new HttpClient { BaseAddress = Server };
			var sourceBytes = Convert.FromBase64String(encrypted);
			var decrypted = ProtectedData.Unprotect(sourceBytes,Entropy,DataProtectionScope.CurrentUser);
			var head = Convert.ToBase64String(decrypted);
			_client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", head);
		}

		void SetupBasicAuth(string username, string password) {
			var s = Convert.ToBase64String(CredentialsToHeader(username, password));
			var header = new AuthenticationHeaderValue("Basic", s);
			_client.DefaultRequestHeaders.Authorization = header;
		}

		static byte[] CredentialsToHeader(string username, string password) {
			return Encoding.ASCII.GetBytes(username + ":" + password);
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