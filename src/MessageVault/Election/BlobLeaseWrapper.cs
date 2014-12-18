using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Serilog;

namespace MessageVault.Election {

	/// <summary>
	///   Wrapper around a Windows Azure Blob Lease
	/// </summary>
	public class BlobLeaseWrapper {
		readonly CloudPageBlob _leaseBlob;
		readonly int _size;
		readonly ILogger _logger;


		public BlobLeaseWrapper(CloudPageBlob leaseBlob, int size) {
			_leaseBlob = leaseBlob;
			_size = size;
			_logger = Log.ForContext<BlobLeaseWrapper>();
		}

		public void ReleaseLease(string leaseId) {
			try {
				_leaseBlob.ReleaseLease(new AccessCondition {LeaseId = leaseId});
			}
			catch (StorageException e) {
				// Lease will eventually be released.
				_logger.Error(e, e.Message);
			}
		}

		public async Task<string> AcquireLeaseAsync(CancellationToken token) {
			bool blobNotFound = false;
			try {
				return await _leaseBlob.AcquireLeaseAsync(Constants.AcquireLeaseFor, null, token);
			}
			catch (StorageException storageException) {
				_logger.Error(storageException, "Failed to get lease. {Error}", storageException.Message);


				var webException = storageException.InnerException as WebException;

				if (webException != null) {
					var response = webException.Response as HttpWebResponse;
					if (response != null) {
						if (response.StatusCode == HttpStatusCode.NotFound) {
							blobNotFound = true;
						}

						if (response.StatusCode == HttpStatusCode.Conflict) {
							return null;
						}
					} else {
						return null;
					}
				}
			}

			if (blobNotFound) {
				await CreateBlobAsync(token);
				return await AcquireLeaseAsync(token);
			}

			return null;
		}

		public async Task<bool> RenewLeaseAsync(string leaseId, CancellationToken token) {
			try {
				await _leaseBlob.RenewLeaseAsync(new AccessCondition {LeaseId = leaseId}, token);
				return true;
			}
			catch (StorageException e) {
				// catch (WebException webException)
				_logger.Error(e, e.Message);


				return false;
			}
		}

		async Task CreateBlobAsync(CancellationToken token) {
			await _leaseBlob.Container.CreateIfNotExistsAsync(token);
			if (!await _leaseBlob.ExistsAsync(token)) {
				try {
					await _leaseBlob.CreateAsync(_size, token);
				}
				catch (StorageException e) {
					if (e.InnerException is WebException) {
						var webException = e.InnerException as WebException;
						var response = webException.Response as HttpWebResponse;

						if (response == null || response.StatusCode != HttpStatusCode.PreconditionFailed) {
							throw;
						}
					}
				}
			}
		}
	}

}