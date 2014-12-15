using System;

namespace MessageVault {

	/// <summary>
	/// Binary-sortable Guid with a custom scheme
	/// </summary>
	public static class Uuid {

		private static readonly DateTime UnixEpoch =
		new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

		//static byte[] _nodeId;

		//static Uuid() {
		//	var nativeGuid = Guid.NewGuid();
		//	var bytes = nativeGuid.ToByteArray();
		//	Array.Copy(bytes, 10, _nodeId,0, 6);
		//}

		static long GetCurrentUnixTimestampMs()
		{
			// unfortunately clock on windows has only millisecond precision :(
			// no nanoseconds
			return (long)(DateTime.UtcNow - UnixEpoch).TotalMilliseconds;
		}

		public static byte[] CreateNew(long offset) {
			var result = new byte[16];
			var date = BitConverter.GetBytes(GetCurrentUnixTimestampMs());

			var offsetBytes = BitConverter.GetBytes(offset);

			Array.Copy(date, result, 8);
			Array.Copy(offsetBytes, 0, result, 8, 8);
			return result;
		}
	}

}