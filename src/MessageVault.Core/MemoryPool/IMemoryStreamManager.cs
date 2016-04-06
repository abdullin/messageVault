using System.IO;

namespace MessageVault.MemoryPool {

	/// <summary>
	/// Replace with RecyclableMemoryStream from Microsoft, if you want to use your pool
	/// </summary>
	public interface IMemoryStreamManager
	{
		MemoryStream GetStream(string tag);
		MemoryStream GetStream(string tag, int length);
	}

}