using System;
using System.Linq;
using MessageVault.Server;
using Microsoft.WindowsAzure.Storage;
using Serilog;

namespace WorkerRole {

	public static class Program {
		public static void Main(params string[] args) {
			InitLogging();

			AppDomain.CurrentDomain.UnhandledException += CurrentDomainOnUnhandledException;

			var range = args.FirstOrDefault() ?? "8";

			var config = new AppConfig {
				PublicUri = "http://127.0.0.1:" + range + "001",
				InternalUri = "http://127.0.0.1:" + range + "002",

				StorageAccount = new SingleAccountFactory(CloudStorageAccount.DevelopmentStorageAccount)
			};

			var app = App.Initialize(config);
			Log.Information(
				"Console app started on {public} and {private}", 
				config.PublicUri, 
				config.InternalUri
				);


			Console.ReadLine();
			app.RequestStop();
			app.GetCompletionTask().Wait();
			Log.Information("App terminated");
			Console.ReadLine();
		}

		static void CurrentDomainOnUnhandledException(object sender,
			UnhandledExceptionEventArgs unhandledExceptionEventArgs) {
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