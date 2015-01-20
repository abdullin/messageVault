using System;

namespace MessageVault {


	public interface ICheckpointReader : IDisposable {
		long Read();
	}

}