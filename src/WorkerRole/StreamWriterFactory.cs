using System.Collections.Concurrent;
using MessageVault;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Serilog;

namespace WorkerRole {

	public sealed class StreamWriterFactory {
		readonly CloudBlobClient _client;

		readonly ConcurrentDictionary<string, SegmentWriter> _writers = new ConcurrentDictionary<string, SegmentWriter>();
		
		
		public static StreamWriterFactory CreateDev() {
			var blob = CloudStorageAccount.DevelopmentStorageAccount.CreateCloudBlobClient();
			return new StreamWriterFactory(blob);
		}
		
		StreamWriterFactory(CloudBlobClient client) {
			_client = client;
		}


		public SegmentWriter Get(string stream) {
			stream = stream.ToLowerInvariant();
			return _writers.GetOrAdd(stream, s => SegmentWriter.Create(_client, stream));
		}

		public void Dispose() {
			Log.Information("Shutting down stream writers");
		}
	}

}