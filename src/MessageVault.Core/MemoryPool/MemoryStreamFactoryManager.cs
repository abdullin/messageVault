using System.IO;

namespace MessageVault.MemoryPool {

	public sealed class MemoryStreamFactoryManager : IMemoryStreamManager {
		public MemoryStream GetStream(string tag) {
			return new MemoryStream();
		}

		public MemoryStream GetStream(string tag, int length) {
			return new MemoryStream();
		}


		public static IMemoryStreamManager Instance = new MemoryStreamFactoryManager();
	}

}