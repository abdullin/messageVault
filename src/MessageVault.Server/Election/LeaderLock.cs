using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Serilog;

namespace MessageVault.Server.Election {

	/// <summary>
	/// Acquires a unique blob lease and runs <see cref="LeaderMethod"/>, while it is a leader.
	/// </summary>
	public sealed class LeaderLock {
		readonly CloudStorageAccount _account;
		readonly LeaderInfo _info;
		readonly ApiImplementation _api;
		readonly RenewableBlobLease _lease;
		
		readonly ILogger _log = Log.ForContext<LeaderLock>();

		
		public LeaderLock(CloudStorageAccount account, LeaderInfo info, ApiImplementation api) {
			Require.NotNull("account", account);
			Require.NotNull("info", info);
			_account = account;
			_info = info;
			_api = api;
			_lease = RenewableBlobLease.Create(account, LeaderMethod);
		}


		public Task KeepTryingToAcquireLock(CancellationToken token) {
			return _lease.RunElectionsForever(token);
		}

		async Task LeaderMethod(CancellationToken token, CloudPageBlob blob) {
			var processors = Environment.ProcessorCount;
			var parallelism = Math.Min(processors * 2, 48);
			_log.Information("Node is a leader with {processors} processors. Setting parallelism to {parallelism}", 
				processors, 
				parallelism);

			using (var scheduler = MessageWriteScheduler.Create(_account, parallelism)) {
				try {
					_log.Information("Message write scheduler created");
					_api.EnableDirectWrites(scheduler);
					
					// tell the world who is the leader
					await _info.WriteToBlob(_account);
					// sleep till cancelled
					await Task.Delay(-1, token);
				}
				catch (OperationCanceledException) {
					// expect this exception to be thrown in normal circumstances or check the cancellation token, because
					// if the lease can't be renewed, the token will signal a cancellation request.
					_log.Information("Shutting down the scheduler");
					// shutdown the scheduler
					_api.DisableDirectWrites();


					var shutdown = scheduler.Shutdown();
					if (shutdown.Wait(5000)) {
						_log.Information("Scheduler is down");
					} else {
						_log.Error("Scheduler failed to shutdown in time");
					}
				}
				finally {
					_api.DisableDirectWrites();
					_log.Information("This node is no longer a leader");
				}
			}
		}


	}

}