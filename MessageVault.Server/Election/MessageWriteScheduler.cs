using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using MessageVault.Cloud;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Serilog;

namespace MessageVault.Server.Election {

	/// <summary>
	/// Ensures that all writes to a single stream are sequential
	/// </summary>
	public sealed class MessageWriteScheduler : IDisposable{
		readonly CloudBlobClient _client;

		readonly ConcurrentDictionary<string, MessageWriter> _writers;
		readonly 	ConcurrentExclusiveSchedulerPair _scheduler;
		readonly TaskFactory _exclusiveFactory;
		
		
		public static MessageWriteScheduler Create(CloudStorageAccount account) {
			var client = account.CreateCloudBlobClient();
			client.DefaultRequestOptions.RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(0.5), 3);
			return new MessageWriteScheduler(client);
		}
		
		MessageWriteScheduler(CloudBlobClient client) {
			_client = client;
			_writers = new ConcurrentDictionary<string, MessageWriter>();
			
			_scheduler = new ConcurrentExclusiveSchedulerPair();
			_exclusiveFactory = new TaskFactory(_scheduler.ExclusiveScheduler);
		}

		public async Task Shutdown() {
			_scheduler.Complete();
			await _scheduler.Completion;
		}


		public Task<long> Append(string stream, ICollection<MessageToWrite> data) {
			
			return _exclusiveFactory.StartNew(() => {
				var segment = Get(stream);
				return segment.Append(data);
			});
		}

		public string GetReadAccessSignature(string stream) {
			return CloudSetup.GetReadAccessSignature(_client, stream);
		}


		MessageWriter Get(string stream) {
			stream = stream.ToLowerInvariant();

			return _writers.GetOrAdd(stream, s => CloudSetup.CreateAndInitWriter(_client, stream));
		}

		public void Dispose() {
			Log.Information("Disposing write scheduler");
		}
	}

}