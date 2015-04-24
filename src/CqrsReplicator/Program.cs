using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using MessageVault;
using MessageVault.Api;
using Serilog;

namespace CqrsReplicator {

	class Program {
		static void Main(string[] args) {}
	}

	public sealed class EventStoreConfig {
		public string Host { get; set; }
		public string Login { get; set; }
		public string Password { get; set; }
	}

	/// <summary>
	///   Is responsible for publishing Lokad.CQRS events to a linked event store
	/// </summary>
	public sealed class EventStorePublisher {
		readonly IAppendOnlyStore _store;
		readonly EventStoreConfig _config;
		readonly ICheckpointWriter _checkpoint;
		readonly string _streamName;

		readonly ILogger _log = Log.ForContext<EventStorePublisher>();

		public EventStorePublisher(
			IAppendOnlyStore store,
			EventStoreConfig config,
			ICheckpointWriter checkpoint,
			string streamName
			) {
			_store = store;
			_config = config;
			_checkpoint = checkpoint;
			_streamName = streamName;
		}

		public void Run(CancellationToken token) {
			var connectFailure = 0;
			while (!token.IsCancellationRequested) {
				try {
					var localStoreIsEmpty = _store.GetCurrentVersion() == 0;
					if (localStoreIsEmpty) {
						token.WaitHandle.WaitOne(TimeSpan.FromSeconds(30));
						continue;
					}

					_log.Information("Starting ES replication to {stream}", _streamName);

					using (var conn = new Client(_config.Host, _config.Login, _config.Password)) {
						connectFailure = 0;

						var lastReplicatedEvent = _checkpoint.GetOrInitPosition();

						while (!token.IsCancellationRequested) {
							if (lastReplicatedEvent == _store.GetCurrentVersion()) {
								// no work to do, so sleep and continue
								token.WaitHandle.WaitOne(500);
								continue;
							}

							var keys = _store.ReadRecords(lastReplicatedEvent, 1000).ToList();
							var remoteEvents = keys.Select(MessageToWrite).ToList();
							conn.PostMessagesAsync(_streamName, remoteEvents).Wait(token);

							lastReplicatedEvent = keys.Last().StoreVersion;
							_checkpoint.Update(lastReplicatedEvent);
						}
					}
				}
				catch (Exception ex) {
					if (connectFailure == 0) {
						_log.Error(ex, "Write connection failure");
					}
					connectFailure += 1;
					token.WaitHandle.WaitOne(TimeSpan.FromMinutes(1));
				}
			}
		}

		static MessageToWrite MessageToWrite(DataWithKey record) {
			// by compressing we trade off some CPU to IO operations.
			// smaller files are faster to download and easier to manage
			using (var stream = new MemoryStream()) {
				using (var zip = new GZipStream(stream, CompressionLevel.Fastest, true)) {
					zip.Write(record.Data, 0, record.Data.Length);
				}
				return new MessageToWrite(MessageFlags.None, record.Key, stream.ToArray());
			}
		}
	}

}