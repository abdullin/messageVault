using System;

namespace MessageVault {

	public static class Constants {
		// this would allow consumers to use fixed-size buffers.
		// Also see Snappy framing format
		// https://code.google.com/p/snappy/source/browse/trunk/framing_format.txt
		
		public const long MaxValueSize = ushort.MaxValue; //65535
		/// <summary>
		/// Tweak this, but keep low. Contract is always read
		/// </summary>
		public const int MaxKeySize = byte.MaxValue; // 256

		public const int MaxBatchSize = ushort.MaxValue;

		public const string PositionFileName = "position.c6";
		/// <summary>
		/// This file name is compatible with future stream splitting
		/// </summary>
		public const string StreamFileName = "000000000000.b6";

		public const string SysContainer = "mv-sys";
		public const string DataContainerPrefix = "mv-";
		public const string MasterLockFileName = "master.lock";
		public const string MasterDataFileName = "master.info";
		public const string AuthFileName = "auth.json";

		public const string ClusterNodeUser = "cluster-node";

		public const string DefaultPassword = "ChangeThisNow!";
		public const string DefaultLogin = "admin";

		/// <summary>
		/// Azure allows leases for 15, 30, 45, 60 or infinite.
		/// http://blogs.msdn.com/b/windowsazurestorage/archive/2012/06/12/new-blob-lease-features-infinite-leases-smaller-lease-times-and-more.aspx
		/// </summary>
		public static readonly TimeSpan AcquireLeaseFor = TimeSpan.FromSeconds(15);
	}

}