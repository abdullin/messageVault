using Serilog;
using Serilog.Events;

namespace MessageVault {
    public class Logging {
        public static void InitTrace() {
	        Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Verbose()

                .WriteTo.Trace()
                .CreateLogger();
        }
    }
}