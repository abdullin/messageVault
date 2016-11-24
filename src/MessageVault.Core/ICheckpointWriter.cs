using System;

namespace MessageVault {

	public interface ICheckpointWriter : IDisposable{
		long GetOrInitPosition();
		long ReadPositionVolatile();
		void Update(long position);

		
	}

}