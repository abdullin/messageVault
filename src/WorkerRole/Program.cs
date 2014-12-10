using System;
using System.Threading.Tasks;
using Serilog;

namespace WorkerRole {

	public static class Program {

		
		public static void Main() {
			InitLogging();


			var app = App.Create("http://127.0.0.1:8888");




			Log.Information("Console app started");



			Console.ReadLine();
			app.RequestStop();
			app.GetCompletionTask().Wait();
			Log.Information("App terminated");
			Console.ReadLine();
		}

		static void InitLogging() {
			Log.Logger = new LoggerConfiguration()
				.MinimumLevel.Verbose()
				.WriteTo.ColoredConsole()
				.CreateLogger();
		}
	}

}