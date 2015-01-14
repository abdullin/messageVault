using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using MessageVault.Cloud;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Serilog;
using StatsdClient;
using System.Linq;

namespace MessageVault.Server.Election {

	/// <summary>
	/// Ensures that all writes to a single stream are sequential
	/// </summary>
	public sealed class MessageWriteScheduler : IDisposable{
		readonly CloudBlobClient _client;

		readonly ConcurrentDictionary<string, MessageWriter> _writers;
		readonly TaskSchedulerWithAffinity _scheduler;


		
		
		
		public static MessageWriteScheduler Create(CloudStorageAccount account, int parallelism) {
			var client = account.CreateCloudBlobClient();
			client.DefaultRequestOptions.RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(0.5), 3);
			
			return new MessageWriteScheduler(client, parallelism);
		}
		
		MessageWriteScheduler(CloudBlobClient client, int parallelism) {
			_client = client;
			_writers = new ConcurrentDictionary<string, MessageWriter>();
			_scheduler = new TaskSchedulerWithAffinity(parallelism);
		}

		public async Task Shutdown() {
			await _scheduler.Shutdown();
		}


		public Task<long> Append(string stream, ICollection<MessageToWrite> data) {
			stream = stream.ToLowerInvariant();
			var hash = stream.GetHashCode();

			return _scheduler.StartNew(hash, () => {
				var segment = Get(stream);
				using (Metrics.StartTimer("storage.append.time")) {
					var append = segment.Append(data);
					Metrics.Counter("storage.append.ok");
					Metrics.Counter("storage.append.events", data.Count);
					Metrics.Counter("storage.append.bytes", data.Sum(mw => mw.Value.Length));
					Metrics.Gauge("stream." + stream, append);
					return append;
				}
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