using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault {

	public sealed class MessageReader {
		readonly PositionReader _position;
		readonly PageReader _messages;

		public static MessageReader Create(string sas) {
			var uri = new Uri(sas);
			var container = new CloudBlobContainer(uri);

			var posBlob = container.GetPageBlobReference("stream.chk");
			var dataBlob = container.GetPageBlobReference("stream.dat");
			var position = new PositionReader(posBlob);
			var messages = new PageReader(dataBlob);
			return new MessageReader(position, messages);

		}

		public MessageReader(PositionReader position, PageReader messages) {
			_position = position;
			_messages = messages;
		}


		public long GetPosition() {
			// TODO: inline readers
			return _position.Read();
		}
		public IEnumerable<StoredMessage> ReadMessages(long start, long offset) {
			return _messages.Read(start, offset);


		}


	}


	public sealed class StoredMessage {
		public readonly Uuid Id;
		public readonly string Contract;
		public readonly byte[] Data;

		public StoredMessage(Uuid id, string contract, byte[] data) {
			Id = id;
			Contract = contract;
			Data = data;
		}
	}

}