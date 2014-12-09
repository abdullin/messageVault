using Serilog;
using Serilog.Events;

namespace MessageVault {
    public class Logging {
        public static void InitTrace() {
            var configuration = new LoggerConfiguration();


            
            Log.Logger = configuration
                .MinimumLevel.Verbose()

                .WriteTo.Trace()
                .CreateLogger();
        }
    }
}