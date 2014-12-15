using System;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault {

	public sealed class MessageReader {
		readonly PositionReader _position;

		public static MessageReader Create(string sas) {
			var uri = new Uri(sas);
			var container = new CloudBlobContainer(uri);

			var posBlob = container.GetPageBlobReference("stream.chk");
			var reader = new PositionReader(posBlob);
			return new MessageReader(reader);

		}

		public MessageReader(PositionReader position) {
			_position = position;
		}


		public long GetPosition() {
			return _position.Read();
		}
	}

}