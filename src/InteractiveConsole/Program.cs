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

				var message = new Message("test", new byte[20]);

				var response = await client.PostMessagesAsync("test", new[] {message});

				Console.WriteLine(response);

				var reader = await client.GetMessageReaderAsync("test");

				Console.WriteLine("Current position is " + reader.GetPosition());

				//var r = client.GetStringAsync("/streams/test");
			}
		}
	}

}