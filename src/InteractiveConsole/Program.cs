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
				var response = await client.PostMessageAsync("test", new byte[10]);

				Console.WriteLine(response);

				//var r = client.GetStringAsync("/streams/test");
			}
		}
	}

}