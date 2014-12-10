using Serilog;

namespace WorkerRole {

	public static class Program {
		public static void Main() {
			InitLogging();

			Log.Information("Console app started");
		}

		static void InitLogging() {
			Log.Logger = new LoggerConfiguration()
				.MinimumLevel.Verbose()
				.WriteTo.ColoredConsole()
				.CreateLogger();
		}
	}

}