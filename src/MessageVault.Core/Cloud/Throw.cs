using System;
using System.Net;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;

namespace MessageVault.Cloud {

	public static class Throw {
		/// <summary>
		///     There are some weird cases, when our file is updated concurrently (or Azure messed up notification about file
		///     update). When this happens, it is better to simply restart.
		/// </summary>
		/// <param name="exec"></param>
		public static void OnEtagMismatchDuringAppend(Action exec) {
			try {
				exec();
			}
			catch (StorageException ex) {
				if (ex.RequestInformation.HttpStatusCode == (int) HttpStatusCode.PreconditionFailed) {
					throw new NonTransientAppendFailure("ETAG failed, must reboot", ex);
				}
				throw;
			}
		}

	}

	/// <summary>
	///     Is thrown when we hit non-transient error. This is an indication for the manager to restart the process.
	/// </summary>
	[Serializable]
	public class NonTransientAppendFailure : Exception {
		public NonTransientAppendFailure() {}
		public NonTransientAppendFailure(string message, Exception inner) : base(message, inner) {}

		protected NonTransientAppendFailure(
			SerializationInfo info,
			StreamingContext context)
			: base(info, context) {}
	}

}