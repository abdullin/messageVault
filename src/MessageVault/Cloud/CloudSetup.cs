using System;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault.Cloud {

	public static class CloudSetup {
		public static MessageWriter CreateAndInit(CloudBlobClient client, string stream) {
			var container = client.GetContainerReference(stream);
			container.CreateIfNotExists();
			var dataBlob = container.GetPageBlobReference(Constants.StreamFileName);
			var posBlob = container.GetPageBlobReference(Constants.PositionFileName);
			var pageWriter = new CloudPageWriter(dataBlob);
			var posWriter = new CloudCheckpointWriter(posBlob);
			var writer = new MessageWriter(pageWriter, posWriter, stream);
			writer.Init();

			return writer;
		}

		public static string GetReadAccessSignature(CloudBlobClient client, string stream) {
			var container = client.GetContainerReference(stream);
			var signature = container.GetSharedAccessSignature(new SharedAccessBlobPolicy {
				Permissions = SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Read, 
				SharedAccessExpiryTime = DateTimeOffset.Now.AddDays(7),
			});
			return container.Uri + signature;
		}
	}

}