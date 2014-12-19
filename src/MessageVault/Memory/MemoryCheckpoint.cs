namespace MessageVault.Memory {

	
	public sealed class MemoryCheckpoint : ICheckpointWriter {
		long _value = 0;
		public long GetOrInitPosition() {
			return _value;
		}

		public void Update(long position) {
			Require.ZeroOrGreater("position", position);
			_value = position;
		}
	}

}