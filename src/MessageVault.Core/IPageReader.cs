using System;
using System.IO;
using System.Threading.Tasks;

namespace MessageVault {

	public interface IPageReader : IDisposable {
		void DownloadRangeToStream(Stream stream, long offset, int length);
		Task DownloadRangeToStreamAsync(Stream stream, long offset, int length);
		//_blob.DownloadRangeToStream(stream, pageOffset, length);
	}

}