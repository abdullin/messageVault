using System;
using System.Threading;
using System.Threading.Tasks;
using MessageVault;


namespace InteractiveConsole {

	// I'm sorry for async stuff here. HttpClient has only async methods.
	// and restSharp doesn't support streaming binary content without multipart encoding
	class Program {
		static void Main(string[] args) {
			Console.WriteLine("Starting");
			Thread.Sleep(1000);
			
			RunAsync().Wait();
			Console.ReadLine();

			
		}


		static async Task RunAsync() {
			using (var client = new Client("http://127.0.0.1:8888")) {


				// consumer
				var checkpoint = new MemoryCheckpoint();
				var consumer = new ConsumerSample(checkpoint, client);

				var task = Task.Run(() => consumer.Run(CancellationToken.None));

				for (int i = 0; i < 10; i++) {
					var message = new IncomingMessage("test", new byte[20]);

					var response = await client.PostMessagesAsync("test", new[] { message });
					Console.WriteLine(response);

					await Task.Delay(1000);

				}
			}
		}
	}

	public sealed class ConsumerSample
	{
		readonly IWriteableCheckpoint _checkpoint;
		readonly Client _client;

		public ConsumerSample(IWriteableCheckpoint checkpoint, Client client)
		{
			_checkpoint = checkpoint;
			_client = client;
		}

		public async void Run(CancellationToken ct)
		{
			var current = _checkpoint.GetOrInitPosition();
			var reader = await _client.GetMessageReaderAsync("test");


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