using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Serilog;

namespace MessageVault.Server.Election {

	/// <summary>
	/// Acquires a unique blob lease and runs <see cref="ManageBeingLeader"/>, while it is a leader.
	/// </summary>
	public sealed class LeaderLock {
		readonly ICloudFactory _account;
		readonly LeaderInfo _info;
		readonly ApiImplementation _api;
		readonly RenewableBlobLease _lease;
		
		readonly ILogger _log = Log.ForContext<LeaderLock>();

		
		public LeaderLock(ICloudFactory account, LeaderInfo info, ApiImplementation api) {
			Require.NotNull("account", account);
			Require.NotNull("info", info);
			_account = account;
			_info = info;
			_api = api;
			_lease = RenewableBlobLease.Create(account, ManageBeingLeader);
		}

		public Task KeepTryingToAcquireLock(CancellationToken token) {
			return _lease.RunElectionsForever(token);
		}

		async Task ManageBeingLeader(CancellationToken token, CloudPageBlob blob) {
			var parallelism = CalculateParallelism();

			using (var scheduler = MessageWriteScheduler.Create(_account, parallelism)) {
				try {
					await StartBeingLeader( token, scheduler );
					// sleep till cancelled
					await Task.Delay(Timeout.Infinite, token);
				}
				catch (OperationCanceledException) {
					// expect this exception to be thrown in normal circumstances or check the cancellation token, because
					// if the lease can't be renewed, the token will signal a cancellation request.
					_log.Information("Shutting down the scheduler");
					// shutdown the scheduler
					StopBeingLeader();

					var shutdown = scheduler.Shutdown();
					if (shutdown.Wait(5000)) {
						_log.Information("Scheduler is down");
					} else {
						_log.Error("Scheduler failed to shutdown in time");
					}
				}
				finally {
					StopBeingLeader();
				}
			}
		}

		int CalculateParallelism() {
			var processors = Environment.ProcessorCount;
			int parallelism = processors / 2;
			if( parallelism < 1 ) {
				parallelism = 1;
			}
			_log.Information( "Node is a leader with {processors} processors. Setting parallelism to {parallelism}",
				processors,
				parallelism );
			return parallelism;
		}

		async Task StartBeingLeader( CancellationToken token, MessageWriteScheduler scheduler ) {
			_log.Information( "Message write scheduler created" );
			_api.EnableDirectWrites( scheduler );

			// tell the world who is the leader
			await _info.WriteToBlobAsync( _account, token );
		}

		void StopBeingLeader() {
			_api.DisableDirectWrites();
			_log.Information("This node is no longer a leader");
		}
	}
}