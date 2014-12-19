namespace MessageVault {

	public sealed class MessageToWrite {
		public readonly string Contract;
		public readonly byte[] Data;

		public MessageToWrite(string contract, byte[] data) {
			Contract = contract;
			Data = data;
		}
	}

}