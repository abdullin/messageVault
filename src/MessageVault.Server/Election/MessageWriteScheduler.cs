using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MessageVault.Cloud;
using Serilog;
using StatsdClient;

namespace MessageVault.Server.Election {

	/// <summary>
	///     Ensures that all writes to a single stream are sequential
	/// </summary>
	public sealed class MessageWriteScheduler : IDisposable {
		readonly ICloudFactory _factory;
		readonly CancellationTokenSource _source;

		readonly ConcurrentDictionary<string, MessageWriter> _writers;
		readonly TaskSchedulerWithAffinity _scheduler;

		public static MessageWriteScheduler Create(ICloudFactory factory, int parallelism, CancellationTokenSource source) {
			return new MessageWriteScheduler(factory, parallelism, source);
		}

		MessageWriteScheduler(ICloudFactory factory, int parallelism, CancellationTokenSource source) {
			_factory = factory;
			_source = source;
			_writers = new ConcurrentDictionary<string, MessageWriter>();
			_scheduler = new TaskSchedulerWithAffinity(parallelism);
		}

		public async Task Shutdown() {
			await _scheduler.Shutdown();
		}


		public Task<AppendResult> Append(string stream, ICollection<Message> data) {
			_source.Token.ThrowIfCancellationRequested();
			stream = stream.ToLowerInvariant();
			var hash = stream.GetHashCode();
			var segment = Get(stream);

			return _scheduler.StartNew(hash, () => {
				using (Metrics.StartTimer("storage.append.time")) {
					try {
						var append = segment.Append(data);

						Metrics.Counter("storage.append.events", data.Count);
						Metrics.Counter("storage.append.bytes", data.Sum(mw => mw.Value.Length));

						// current position of a stream
						Metrics.Gauge("stream." + stream, append.Position);

						// number of appends to a stream
						Metrics.Counter("stream." + stream + ".append.ok");
						Metrics.Counter("stream." + stream + ".append.events", data.Count);

						return append;
					}
					catch (PanicException) {
						_source.Cancel();
						throw;
					}

				}
			});
		}

		public string GetReadAccessSignature(string stream) {
			var container = _factory.GetContainerReference(stream);
			return CloudSetup.GetReadAccessSignature(container);
		}


		MessageWriter Get(string stream) {

			_source.Token.ThrowIfCancellationRequested();
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