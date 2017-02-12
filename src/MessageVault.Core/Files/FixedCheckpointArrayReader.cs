namespace MessageVault.Files {

	public sealed class FixedCheckpointArrayReader : IVolatileCheckpointVectorAccess {
		public readonly long[] Vector;
		public FixedCheckpointArrayReader(long[] vector) {
			Vector = vector;
		}

		public long[] ReadPositionVolatile() {
			return Vector;
		}
	}

}