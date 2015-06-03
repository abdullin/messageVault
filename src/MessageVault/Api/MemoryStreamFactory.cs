using System.IO;

namespace MessageVault.Api {

	public sealed class MemoryStreamFactory : IMemoryStreamManager {
		public MemoryStream GetStream(string tag) {
			return new MemoryStream();
		}

		public MemoryStream GetStream(string tag, int length) {
			return new MemoryStream();
		}
	}

}