using System;
using System.Diagnostics.Contracts;

namespace MessageVault {

	public static class Require {
		public static void ZeroOrGreater(string param, long value) {
			Contract.Requires(value >=0);
			if (value < 0) {
				throw new ArgumentOutOfRangeException(param, value, "Must be zero or greater");
			}
		}
		public static void Positive(string param, long value) {
			Contract.Requires(value>0);
			if (value <= 0)
			{
				throw new ArgumentOutOfRangeException(param, value, "Must be greater than zero");
			}
		}
	}

	public static class Ensure {
		public static void ZeroOrGreater(string param, long value) {
			Contract.Requires(value >= 0);
			if (value < 0) {
				throw new InvalidOperationException("Value must be zero or greater");
			}
		}
	}

}