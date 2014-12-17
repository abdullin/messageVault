using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Serilog;

namespace MessageVault.Election {

	public sealed class LeaderPublisher {
		readonly string _endpoint;
		readonly RenewableBlobLease _lease;
		bool _isLeader;
		readonly ILogger _log = Log.ForContext<LeaderPublisher>();

		public bool IsLeader() {
			return _isLeader;
		}

		public LeaderPublisher(CloudStorageAccount account, string endpoint) {
			_endpoint = endpoint;
			_lease = RenewableBlobLease.Create(account, LeaderMethod);
		}


		public void Run(CancellationToken token) {
			_lease.RunElectionsForever(token).Wait();
		}

		async Task LeaderMethod(CancellationToken token, CloudPageBlob blob) {
			try {
				while (!token.IsCancellationRequested) {
					_isLeader = true;
					_log.Information("This node is a leader");
					
					// tell the world who is the leader
					await WriteLeaderInfo(blob);
					// sleep for some time or until shutdown signal 
					// (because lease is lost or we shutdown)
					await Task.Delay(TimeSpan.FromMinutes(10), token);
				}
			}
			catch (OperationCanceledException) {
				// expect this exception to be thrown in normal circumstances or check the cancellation token, because
				// if the lease can't be renewed, the token will signal a cancellation request.
				_log.Information("Aborting work, as lease been lost");
			}
			finally {
				_isLeader = false;
				_log.Information("This node is no longer a leader");
			}
		}

		async Task WriteLeaderInfo(CloudPageBlob blob) {
			using (var mem = new MemoryStream(512)) {
				using (var bin = new BinaryWriter(mem, Encoding.UTF8, true)) {
					bin.Write(_endpoint);
				}

				mem.Seek(0, SeekOrigin.Begin);
				await blob.WritePagesAsync(mem, 0, null);
			}
		}
	}

}