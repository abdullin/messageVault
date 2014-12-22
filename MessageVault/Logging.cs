using System;
using System.Diagnostics;
using System.IO;
using Serilog;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Formatting.Display;

namespace MessageVault {

	public static class Logging {
		/// <summary>
		///   Local setup method, copying functions from Serilog.FullFx
		/// </summary>
		public static void InitTrace() {
			Log.Logger = new LoggerConfiguration()
				.MinimumLevel.Verbose()
				.WriteTo.LocalTraceImplementation()
				.CreateLogger();
		}

		public static LoggerConfiguration LocalTraceImplementation(this LoggerSinkConfiguration sinkConfiguration,
			LogEventLevel restrictedToMinimumLevel = LogEventLevel.Verbose) {
			const string outputTemplate =
				"{Timestamp:HH:mm:ss.fff} [{Level}] {Message}{NewLine}{Exception}";
			if (sinkConfiguration == null) {
				throw new ArgumentNullException("sinkConfiguration");
			}

			var templateTextFormatter = new MessageTemplateTextFormatter(outputTemplate, null);
			return sinkConfiguration.Sink(
				new DiagnosticTraceSink(templateTextFormatter),
				restrictedToMinimumLevel);
		}


		internal class DiagnosticTraceSink : ILogEventSink {
			readonly ITextFormatter _textFormatter;

			public DiagnosticTraceSink(ITextFormatter textFormatter) {
				if (textFormatter == null) {
					throw new ArgumentNullException("textFormatter");
				}
				_textFormatter = textFormatter;
			}

			public void Emit(LogEvent logEvent) {
				if (logEvent == null) {
					throw new ArgumentNullException("logEvent");
				}
				var stringWriter = new StringWriter();
				_textFormatter.Format(logEvent, stringWriter);
				Trace.WriteLine(stringWriter.ToString().Trim());
			}
		}
	}

}