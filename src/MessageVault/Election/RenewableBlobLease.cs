using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Serilog;

namespace MessageVault.Election {

	public class RenewableBlobLease {
		/// <summary>
		/// Leader node runs this task, while it is still a leader.
		/// </summary>
		/// <param name="token">watch for this token for shutdown or lost elections</param>
		/// <param name="blob">locked blob</param>
		/// <returns></returns>
		public delegate Task LeaderTask(CancellationToken token, CloudPageBlob blob);

		static readonly TimeSpan RenewInterval = TimeSpan.FromSeconds(4.5);
		static readonly TimeSpan AcquireAttemptInterval = TimeSpan.FromSeconds(6.5);

		readonly CloudPageBlob _blob;
		readonly LeaderTask _leaderTask;
		readonly ILogger _log = Log.ForContext<RenewableBlobLease>();

		public static RenewableBlobLease Create(CloudStorageAccount account, LeaderTask task) {
			Require.NotNull("account", account);
			Require.NotNull("task", task);

			var client = account.CreateCloudBlobClient();
			client.DefaultRequestOptions.RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(1), 3);
			var container = client.GetContainerReference(Constants.LockContainer);
			var blob = container.GetPageBlobReference(Constants.MasterLockFileName);
			return new RenewableBlobLease(blob, task);
		}

		public RenewableBlobLease(CloudPageBlob blob, LeaderTask leaderTask) {
			_blob = blob;
			_leaderTask = leaderTask;
		}


		public async Task RunElectionsForever(CancellationToken token) {
			var leaseWrapper = new BlobLeaseWrapper(_blob, 512);

			while (!token.IsCancellationRequested) {
				await GrabLeaseOrWait(leaseWrapper, token);
			}
		}

		async Task CancelAllWhenAnyCompletes(Task leaderTask, Task renewLeaseTask,
			CancellationTokenSource cts) {
			await Task.WhenAny(leaderTask, renewLeaseTask);

			// Cancel the user's leader task or the renewLease Task, as it is no longer the leader.
			cts.Cancel();

			var allTasks = Task.WhenAll(leaderTask, renewLeaseTask);
			try {
				await Task.WhenAll(allTasks);
			}
			catch (Exception) {
				if (allTasks.Exception != null) {
					allTasks.Exception.Handle(ex => {
						if (!(ex is OperationCanceledException)) {
							_log.Error(ex, "Failure on cancel");
						}

						return true;
					});
				}
			}
		}

		async Task GrabLeaseOrWait(BlobLeaseWrapper leaseWrapper, CancellationToken token) {
			// Try to acquire the blob lease, otherwise wait for some time before we can try again.
			var leaseId = await TryAcquireLeaseOrWait(leaseWrapper, token);

			if (string.IsNullOrEmpty(leaseId)) {
				return;
			}
			// Create a new linked cancellation token source, so if either the 
			// original token is canceled or the lease cannot be renewed,
			// then the leader task can be canceled.
			using (var cts =
				CancellationTokenSource.CreateLinkedTokenSource(new[] {token})) {
				// Run the leader task.
				var leaderTask = _leaderTask.Invoke(cts.Token, _blob);

				// Keeps renewing the lease in regular intervals. 
				// If the lease cannot be renewed, then the task completes.
				var renew  = KeepRenewingLease(leaseWrapper, leaseId, cts.Token);

				// When any task completes (either the leader task or when it could
				// not renew the lease) then cancel the other task.
				await CancelAllWhenAnyCompletes(leaderTask, renew, cts);
			}
		}

		static async Task<string> TryAcquireLeaseOrWait(BlobLeaseWrapper leaseWrapper,
			CancellationToken token) {
			try {
				var leaseId = await leaseWrapper.AcquireLeaseAsync(token);
				if (!string.IsNullOrEmpty(leaseId)) {
					return leaseId;
				}

				await Task.Delay(AcquireAttemptInterval, token);
				return null;
			}
			catch (OperationCanceledException) {
				return null;
			}
		}

		static async Task KeepRenewingLease(BlobLeaseWrapper leaseWrapper, string leaseId,
			CancellationToken token) {
			var renewOffset = new Stopwatch();

			while (!token.IsCancellationRequested) {
				try {
					// Immediately attempt to renew the lease
					// We cannot be sure how much time has passed since the lease was actually acquired
					renewOffset.Restart();
					var renewed = await leaseWrapper.RenewLeaseAsync(leaseId, token);
					renewOffset.Stop();

					if (!renewed) {
						return;
					}

					// We delay based on the time from the start of the last renew request to ensure
					var renewIntervalAdjusted = RenewInterval - renewOffset.Elapsed;

					// If the adjusted interval is greater than zero wait for that long
					if (renewIntervalAdjusted > TimeSpan.Zero) {
						await Task.Delay(RenewInterval - renewOffset.Elapsed, token);
					}
				}
				catch (OperationCanceledException) {
					leaseWrapper.ReleaseLease(leaseId);

					return;
				}
			}
		}
	}

}