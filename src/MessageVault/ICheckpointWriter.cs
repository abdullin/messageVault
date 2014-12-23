namespace MessageVault {

	public interface ICheckpointWriter {
		long GetOrInitPosition();
		void Update(long position);
	}

}