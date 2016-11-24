using System;
using System.IO;
using MessageVault.Files;
using MessageVault.MemoryPool;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace MessageVault.Cloud {

	public static class CloudSetup {

		public static IRetryPolicy RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(0.5), 3);

		public const string CheckpointMetadataName = "position";

		public static MessageWriter CreateAndInitWriter(CloudBlobContainer container) {
			
			container.CreateIfNotExists();
			var dataBlob = container.GetPageBlobReference(Constants.StreamFileName);
			var posBlob = container.GetPageBlobReference(Constants.PositionFileName);
			var pageWriter = new CloudPageWriter(dataBlob);
			var posWriter = new CloudCheckpointWriter(posBlob);
			var writer = new MessageWriter(pageWriter, posWriter);
			writer.Init();

			return writer;
		}

		public static string GetReadAccessSignature(CloudBlobContainer container) {
			
			var signature = container.GetSharedAccessSignature(new SharedAccessBlobPolicy {
				Permissions = SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Read, 
				// since Microsoft servers don't have an uptime longer than a year
				SharedAccessExpiryTime = DateTimeOffset.Now.AddYears(7),
			});
			return container.Uri + signature;
		}

		public static Tuple<CloudCheckpointReader, CloudPageReader> GetReaderRaw(string sas) {
			var uri = new Uri(sas);
			var container = new CloudBlobContainer(uri);

			var posBlob = container.GetPageBlobReference(Constants.PositionFileName);
			var dataBlob = container.GetPageBlobReference(Constants.StreamFileName);
			var position = new CloudCheckpointReader(posBlob);
			var messages = new CloudPageReader(dataBlob);
			return Tuple.Create(position, messages);
		}

		public static MessageReader GetReader(string sas) {
			var raw = GetReaderRaw(sas);
			return new MessageReader(raw.Item1, raw.Item2);
		}

		public static MessageFetcher MessageFetcher(string sas, string stream, IMemoryStreamManager streamManager = null)
		{
			var manager = streamManager ?? new MemoryStreamFactoryManager();
			var raw = GetReaderRaw(sas);
			var remote = raw.Item2;
			var remotePos = raw.Item1;

			return new MessageFetcher(remote, remotePos, manager, stream);
		}

		public static MessageCopier CopyToFiles(string sas, string stream, DirectoryInfo target,
			IMemoryStreamManager streamManager = null) {

			var manager = streamManager ?? new MemoryStreamFactoryManager();
			var raw = GetReaderRaw(sas);
			var remote = raw.Item2;
			var remotePos = raw.Item1;

			var writer = FileSetup.CreateAndInitWriterRaw(target, stream);

			return new MessageCopier(remote, remotePos, manager, writer.Item2, writer.Item1);
		}
	}

}