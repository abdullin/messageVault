using Serilog;

namespace MessageVault {
    public class Logging {
        public static void Init() {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.ColoredConsole()
                .CreateLogger();
        }
    }
}