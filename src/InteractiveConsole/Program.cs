using System;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using MessageVault;
using MessageVault.Api;
using MessageVault.Memory;
using MessageVault.Server.Auth;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;


namespace InteractiveConsole {

	// I'm sorry for async stuff here. HttpClient has only async methods.
	// and restSharp doesn't support streaming binary content without multipart encoding
	class Program {
		static void Main(string[] args) {

			//var auth = new AuthData();
			//auth.Users.Add("tester", new UserInfo() {
			//	Password = "pass",
			//	Claims = new[]{"test:write"}
			//});
			//Console.WriteLine(auth.Serialize());
			//Console.ReadLine();


			Console.WriteLine("Starting");
			Thread.Sleep(1000);
			
			RunAsync().Wait();
			Console.ReadLine();

			
		}


		static async Task RunAsync() {
			using (var client = new CloudClient("http://127.0.0.1:8001", Constants.DefaultLogin, Constants.DefaultPassword)) {
				
				// consumer
				var checkpoint = new MemoryCheckpointReaderWriter();
				var consumer = new ConsumerSample(checkpoint, client);

				var task = Task.Run(() => consumer.Run(CancellationToken.None));

				for (int i = 0; i < 10; i++) {
					var message = Message.Create("test", new byte[20]);

					var response = await client.PostMessagesAsync("teststream", new[] { message });
					Console.WriteLine(response.Position);

					await Task.Delay(1000);

				}

				await task;
			}
		}
	}

	public sealed class ConsumerSample
	{
		readonly ICheckpointWriter _checkpoint;
		readonly CloudClient _cloudClient;

		public ConsumerSample(ICheckpointWriter checkpoint, CloudClient cloudClient)
		{
			_checkpoint = checkpoint;
			_cloudClient = cloudClient;
		}

		public async void Run(CancellationToken ct)
		{
			var current = _checkpoint.GetOrInitPosition();
			var reader = await _cloudClient.GetMessageReaderAsync("test");


			while (!ct.IsCancellationRequested)
			{
				var result = await reader.GetMessagesAsync(ct, current, 100);
				if (result.HasMessages()) {
					foreach (var message in result.Messages) {
						Console.WriteLine("Got message! " + message.Id);
					}
					current = result.NextOffset;
					_checkpoint.Update(current);
				}
			}
		}
	}


}