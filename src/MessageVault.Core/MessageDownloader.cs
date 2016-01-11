using System;

namespace MessageVault {

	public sealed class MessageDownloader : IDisposable {
		readonly ICheckpointReader _position;
		readonly IPageReader _page;

		readonly ICheckpointWriter _outputPosition;
		readonly IPageWriter _writer;


		public void Sync() {
			


		}


		public void Dispose() {
			throw new NotImplementedException();
		}
	}

}