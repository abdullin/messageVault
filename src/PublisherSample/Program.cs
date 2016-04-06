using System;
using System.Threading.Tasks;
using MessageVault;
using MessageVault.Api;

namespace PublisherSample {

	class Program {
		static void Main(string[] args) {
			const string url = "http://127.0.0.1:8001";
			const string login = Constants.DefaultLogin;
			const string password = Constants.DefaultPassword;

			using (var client = new CloudClient(url, login, password)) {
				KeepPostingForever(client).Wait();
			}
		}

		public static async Task KeepPostingForever(CloudClient cloudClient) {
			while (true) {
				var message = Message.Create("test", new byte[20]);

				var response = await cloudClient.PostMessagesAsync("test", new[] {message});
				Console.WriteLine("Wrote at position {0}", response.Position);

				await Task.Delay(1000);
			}
		}
	}

}