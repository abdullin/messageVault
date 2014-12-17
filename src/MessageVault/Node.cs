using System.Threading;

namespace MessageVault {

	public sealed class Node {
		NodeState _state = NodeState.Undefined;

		public NodeState GetState() {
			return _state;
		}

		public void Run(CancellationToken token) {
			while (!token.IsCancellationRequested) {
				
			}
			
		}
	}

	public sealed class LeaseBlob {
		
	}

	public enum NodeState {
		Undefined,
		Master,
		Slave
	}
	

}