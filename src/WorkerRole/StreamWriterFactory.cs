using System.Collections.Concurrent;
using MessageVault;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Serilog;

namespace WorkerRole {

	public sealed class StreamWriterFactory {
		readonly CloudBlobContainer _container;

		readonly ConcurrentDictionary<string, SegmentWriter> _writers = new ConcurrentDictionary<string, SegmentWriter>();
		
		
		public static StreamWriterFactory CreateDev() {
			var blob = CloudStorageAccount.DevelopmentStorageAccount.CreateCloudBlobClient();
			var container = blob.GetContainerReference("dev-vault");
			container.CreateIfNotExists();
			return new StreamWriterFactory(container);
		}
		
		StreamWriterFactory(CloudBlobContainer container) {
			_container = container;
		}


		public SegmentWriter Get(string stream) {
			stream = stream.ToLowerInvariant();
			return _writers.GetOrAdd(stream, s => SegmentWriter.Create(_container, stream));
		}

		public void Dispose() {
			Log.Information("Shutting down stream writers");
		}
	}

}