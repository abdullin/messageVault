using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using MessageVault;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Serilog;

namespace WorkerRole {

	public sealed class StreamScheduler {
		readonly CloudBlobClient _client;

		readonly ConcurrentDictionary<string, SegmentWriter> _writers;
		readonly 	ConcurrentExclusiveSchedulerPair _scheduler;
		readonly TaskFactory _exclusiveFactory;
		readonly Task _completionTask;
		
		public static StreamScheduler CreateDev() {
			var blob = CloudStorageAccount
				.DevelopmentStorageAccount
				.CreateCloudBlobClient();

			blob.DefaultRequestOptions.RetryPolicy = new NoRetry();
			return new StreamScheduler(blob);
		}
		
		StreamScheduler(CloudBlobClient client) {
			_client = client;
			_writers = new ConcurrentDictionary<string, SegmentWriter>();
			
			_scheduler = new ConcurrentExclusiveSchedulerPair();
			_exclusiveFactory = new TaskFactory(_scheduler.ExclusiveScheduler);

			_completionTask = _scheduler.Completion.ContinueWith(task => Dispose());

		}

		public Task GetCompletionTask() {
			return _completionTask;
		}
		public void RequestShutdown() {
			_scheduler.Complete();
		}


		public Task<long> Append(string stream, ICollection<IncomingMessage> data) {
			
			return _exclusiveFactory.StartNew(() => {
				var segment = Get(stream);
				return segment.Append(data);
			});
		}

		public string GetReadAccessSignature(string stream) {
			return Get(stream).GetReadAccessSignature();
		}


		SegmentWriter Get(string stream) {
			stream = stream.ToLowerInvariant();
			return _writers.GetOrAdd(stream, s => SegmentWriter.Create(_client, stream));
		}

		public void Dispose() {
			Log.Information("Shutting down stream writers");
		}
	}

}