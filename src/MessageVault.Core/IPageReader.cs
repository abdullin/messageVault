using System;
using System.IO;

namespace MessageVault {

	public interface IPageReader : IDisposable {
		void DownloadRangeToStream(Stream stream, long offset, int length);
		//_blob.DownloadRangeToStream(stream, pageOffset, length);
	}

}