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
		readonly ICloudFactory _factory;

		readonly ConcurrentDictionary<string, MessageWriter> _writers;
		readonly TaskSchedulerWithAffinity _scheduler;


		
		
		
		public static MessageWriteScheduler Create(ICloudFactory factory, int parallelism) {
			
			return new MessageWriteScheduler(factory, parallelism);
		}
		
		MessageWriteScheduler(ICloudFactory factory, int parallelism) {
			_factory = factory;
			_writers = new ConcurrentDictionary<string, MessageWriter>();
			_scheduler = new TaskSchedulerWithAffinity(parallelism);
		}

		public async Task Shutdown() {
			await _scheduler.Shutdown();
		}


		public Task<long> Append(string stream, ICollection<MessageToWrite> data) {
			stream = stream.ToLowerInvariant();
			var hash = stream.GetHashCode();
			var segment = Get(stream);

			return _scheduler.StartNew(hash, () => {
				
				using (Metrics.StartTimer("storage.append.time")) {
					var append = segment.Append(data);
					
					Metrics.Counter("storage.append.events", data.Count);
					Metrics.Counter("storage.append.bytes", data.Sum(mw => mw.Value.Length));

					// current position of a stream
					Metrics.Gauge("stream." + stream, append);

					// number of appends to a stream
					Metrics.Counter("stream." + stream + ".append.ok");

					return append;
				}
			});
		}

		public string GetReadAccessSignature(string stream) {
			var container = _factory.GetContainerReference(stream);
			return CloudSetup.GetReadAccessSignature(container);
		}


		MessageWriter Get(string stream) {
			stream = stream.ToLowerInvariant();

			return _writers.GetOrAdd(stream, s => {
				var container = _factory.GetContainerReference(stream);
				return CloudSetup.CreateAndInitWriter(container);
			});
		}

		public void Dispose() {
			Log.Information("Disposing write scheduler");
		}
	}

}