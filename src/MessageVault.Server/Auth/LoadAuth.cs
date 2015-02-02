using Serilog;

namespace MessageVault.Server.Auth {

	public static class LoadAuth {
		static AuthData GetEmptyConfig() {
			var log = Log.ForContext<AuthData>();

			log.Warning(
				"Auth JSON file {blob} doesn't exist in container {container}. Using default login/password",
				Constants.AuthFileName,
				Constants.SysContainer
				);

			return AuthData.Default();
		}

		public static AuthData LoadFromStorageAccount(ICloudFactory account) {
			var container = account.GetSysContainerReference();
			var blob = container.GetBlockBlobReference(Constants.AuthFileName);
			if (!blob.Exists()) {
				return GetEmptyConfig();
			}
			var source = blob.DownloadText();
			return AuthData.Deserialize(source);
		}
	}

}