using System;
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
				SharedAccessExpiryTime = DateTimeOffset.Now.AddDays(7),
			});
			return container.Uri + signature;
		}

		public static MessageReader GetReader(string sas) {
			var uri = new Uri(sas);
			var container = new CloudBlobContainer(uri);

			var posBlob = container.GetPageBlobReference(Constants.PositionFileName);
			var dataBlob = container.GetPageBlobReference(Constants.StreamFileName);
			var position = new CloudCheckpointReader(posBlob);
			var messages = new CloudPageReader(dataBlob);
			return new MessageReader(position, messages);
		}
	}

}