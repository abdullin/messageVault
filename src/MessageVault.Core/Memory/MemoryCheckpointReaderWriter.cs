using System.Threading;
using System.Threading.Tasks;

namespace MessageVault.Memory {

	
	public sealed class MemoryCheckpointReaderWriter : ICheckpointWriter, ICheckpointReader {
		long _value;
		public long GetOrInitPosition() {
			return Thread.VolatileRead(ref _value);
		}

		public long ReadPositionVolatile() {
			return Read();
		}

		public void Update(long position) {
			Require.ZeroOrGreater("position", position);
			Thread.VolatileWrite(ref _value, position);
		}

		public long Read() {
			return Thread.VolatileRead(ref _value);
		}

		public Task<long> ReadAsync(CancellationToken token) {
			return Task.FromResult(Read());
		}

		public void Dispose() {
	        
	    }
	}

}