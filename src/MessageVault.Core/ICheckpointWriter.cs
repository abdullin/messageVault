using System;

namespace MessageVault {

	public interface ICheckpointWriter : IDisposable{
		long GetOrInitPosition();
		void Update(long position);
	}

}