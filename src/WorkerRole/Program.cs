using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Serilog;

namespace WorkerRole {

	public static class Program {

		
		public static void Main() {
			InitLogging();

			AppDomain.CurrentDomain.UnhandledException += CurrentDomainOnUnhandledException;

			var config = new AppConfig {
				InternalUri = "http://127.0.0.1:8801",
				PublicUri = "http://127.0.0.1:8001",
				StorageAccount = CloudStorageAccount.DevelopmentStorageAccount
			};


			var app = App.Initialize(config);




			Log.Information("Console app started");



			Console.ReadLine();
			app.RequestStop();
			app.GetCompletionTask().Wait();
			Log.Information("App terminated");
			Console.ReadLine();
		}

		static void CurrentDomainOnUnhandledException(object sender, UnhandledExceptionEventArgs unhandledExceptionEventArgs) {
			Log.Fatal(unhandledExceptionEventArgs.ExceptionObject.ToString());
		}

		static void InitLogging() {
			Log.Logger = new LoggerConfiguration()
				.MinimumLevel.Verbose()
				.WriteTo.ColoredConsole()
				.CreateLogger();
		}
	}

}