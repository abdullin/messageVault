using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MessageVault;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Serilog;

namespace WorkerRole {

	public sealed class StreamScheduler {
		readonly CloudBlobClient _client;

		readonly ConcurrentDictionary<string, MessageWriter> _writers;
		readonly 	ConcurrentExclusiveSchedulerPair _scheduler;
		readonly TaskFactory _exclusiveFactory;
		
		
		public static StreamScheduler CreateDev() {
			var blob = CloudStorageAccount
				.DevelopmentStorageAccount
				.CreateCloudBlobClient();

			blob.DefaultRequestOptions.RetryPolicy = new NoRetry();
			return new StreamScheduler(blob);
		}
		
		StreamScheduler(CloudBlobClient client) {
			_client = client;
			_writers = new ConcurrentDictionary<string, MessageWriter>();
			
			_scheduler = new ConcurrentExclusiveSchedulerPair();
			_exclusiveFactory = new TaskFactory(_scheduler.ExclusiveScheduler);

		}

		public async Task Run(CancellationToken token) {
			while (!token.IsCancellationRequested) {
				await Task.Delay(TimeSpan.MaxValue, token);
			}
			_scheduler.Complete();
			await _scheduler.Completion;
			Dispose();
		}


		public Task<long> Append(string stream, ICollection<IncomingMessage> data) {
			
			return _exclusiveFactory.StartNew(() => {
				var segment = Get(stream);
				return segment.Append(data);
			});
		}

		public string GetReadAccessSignature(string stream) {
			return MessageWriter.GetReadAccessSignature(_client, stream);
		}


		MessageWriter Get(string stream) {
			stream = stream.ToLowerInvariant();

			return _writers.GetOrAdd(stream, s => MessageWriter.Create(_client, stream));
		}

		public void Dispose() {
			Log.Information("Shutting down stream writers");
		}
	}

}