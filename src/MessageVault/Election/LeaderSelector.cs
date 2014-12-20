using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Serilog;

namespace MessageVault.Election {

	public sealed class LeaderSelector {
		readonly CloudStorageAccount _account;
		readonly NodeInfo _info;
		readonly RenewableBlobLease _lease;
		bool _isLeader;
		readonly ILogger _log = Log.ForContext<LeaderSelector>();

		public bool IsLeader() {
			return _isLeader;
		}

		public LeaderSelector(CloudStorageAccount account, NodeInfo info) {
			Require.NotNull("account", account);
			Require.NotNull("info", info);
			_account = account;
			_info = info;
			_lease = RenewableBlobLease.Create(account, LeaderMethod);
		}


		public Task Run(CancellationToken token) {
			return _lease.RunElectionsForever(token);
		}

		async Task LeaderMethod(CancellationToken token, CloudPageBlob blob) {
			_log.Information("This node is a leader");
			using (var scheduler = MessageWriteScheduler.Create(_account)) {
				try {
					_log.Information("Message write scheduler created");
					_isLeader = true;
					// tell the world who is the leader
					await _info.WriteToBlob(_account);
					// sleep till cancelled
					await Task.Delay(-1, token);
				}
				catch (OperationCanceledException) {
					// expect this exception to be thrown in normal circumstances or check the cancellation token, because
					// if the lease can't be renewed, the token will signal a cancellation request.
					_log.Information("Leadership lost. Shutting down the scheduler");
					// shutdown the scheduler
					_isLeader = false;


					var shutdown = scheduler.Shutdown();
					if (shutdown.Wait(5000)) {
						_log.Information("Scheduler is down");
					} else {
						_log.Error("Scheduler failed to shutdown in time");
					}
				}
				finally {
					_isLeader = false;
					_log.Information("This node is no longer a leader");
				}
			}
		}


	}

	public sealed class NodeInfo {

		readonly string _internalEndpoint;
		

		public NodeInfo(string internalEndpoint) {
			_internalEndpoint = internalEndpoint;
		}

		public async Task WriteToBlob(CloudStorageAccount storage) {

			var container = storage.CreateCloudBlobClient().GetContainerReference(Constants.LockContainer);

			var blob = container.GetPageBlobReference(Constants.MasterDataFileName);
			if (!blob.Exists()) {
				blob.Create(512);
			}
			var buffer = new byte[512];
			using (var mem = new MemoryStream(buffer))
			{
				using (var bin = new BinaryWriter(mem, Encoding.UTF8, true))
				{
					bin.Write(_internalEndpoint);
				}

				mem.Seek(0, SeekOrigin.Begin);
				
				await blob.WritePagesAsync(mem, 0, null);
			}
		}
	}

}