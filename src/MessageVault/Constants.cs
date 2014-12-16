namespace MessageVault {

	public static class Constants {
		// this would allow consumers to use fixed-size buffers.
		// Also see Snappy framing format
		// https://code.google.com/p/snappy/source/browse/trunk/framing_format.txt
		public const long MaxMessageSize = 65536;
		/// <summary>
		/// Tweak this, but keep low. Contract is always read
		/// </summary>
		public const int MaxContractLength = 256;

		public const byte ReservedFormatVersion = 0x01;

		public const string PositionFileName = "position";
		/// <summary>
		/// This file name is compatible with future stream splitting
		/// </summary>
		public const string StreamFileName = "000000000000.bin";
	}

}