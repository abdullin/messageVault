using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault {

	public sealed class PageReader {
		readonly CloudPageBlob _blob;
		public PageReader(CloudPageBlob blob) {
			_blob = blob;
		}

		public Stream OpenRead(long from) {
			var stream = _blob.OpenRead();
			stream.Seek(from, SeekOrigin.Begin);
			return stream;

		}
	}

}