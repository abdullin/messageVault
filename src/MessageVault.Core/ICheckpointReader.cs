using System;
using System.Runtime.Serialization;

namespace MessageVault {


	public interface ICheckpointReader : IDisposable {
		long Read();
	}

	[Serializable]
	public class ForbiddenException : Exception {
		//
		// For guidelines regarding the creation of new exception types, see
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
		// and
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
		//

		public ForbiddenException() {}
		public ForbiddenException(string message) : base(message) {}
		public ForbiddenException(string message, Exception inner) : base(message, inner) {}

		protected ForbiddenException(
			SerializationInfo info,
			StreamingContext context) : base(info, context) {}
	}

}