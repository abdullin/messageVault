using System;
using System.Diagnostics;
using System.Diagnostics.Contracts;

namespace MessageVault {

	public static class Require {
		[DebuggerNonUserCode]
		public static void OffsetMultiple(string param, long value, int multiple) {
			Contract.Requires(value >= 0);
			Contract.Requires(value % multiple == 0);

			if (value < 0) {
				const string message = "Offset can't be negative";
				throw new ArgumentOutOfRangeException(param, value, message);
			}
			if (value % multiple != 0) {
				var message = "Offset must be divisible by " + multiple;
				throw new ArgumentOutOfRangeException(param, value, message);
			}
		}


		[DebuggerNonUserCode]
		public static void ZeroOrGreater(string param, long value) {
			Contract.Requires(value >=0);
			if (value < 0) {
				throw new ArgumentOutOfRangeException(param, value, "Must be zero or greater");
			}
		}
		[DebuggerNonUserCode]
		public static void Positive(string param, long value) {
			Contract.Requires(value>0);
			if (value <= 0)
			{
				throw new ArgumentOutOfRangeException(param, value, "Must be greater than zero");
			}
		}
		[DebuggerNonUserCode]
		public static void NotNull<T>(string param, T value) where T:class{
			Contract.Requires(value != null);
			if (value == null) {
				throw new ArgumentNullException(param);
			}
			
		}
	}

	public static class Ensure {
		[DebuggerNonUserCode]
		public static void ZeroOrGreater(string param, long value) {
			Contract.Requires(value >= 0);
			if (value < 0) {
				throw new InvalidOperationException("Value must be zero or greater. Got " + value);
			}
		}
	}

}