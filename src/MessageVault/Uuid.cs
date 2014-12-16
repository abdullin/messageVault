using System;
using System.Diagnostics.Contracts;
using NUnit.Framework;

namespace MessageVault {

	/// <summary>
	/// Binary-sortable uuid with a custom scheme
	/// [timestamp:8] - [stream-offset:8]
	/// </summary>
	public struct Uuid {

		public static readonly DateTime Epoch =
		new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);


		readonly long _time;
		readonly long _offset;
		[Pure]
		public DateTime GetTimeUtc() {
			return Epoch.AddMilliseconds(_time);
		}
		[Pure]
		public long GetOffset() {
			return _offset;
		}

		public Uuid(long time, long offset) {
			_time = time;
			_offset = offset;
		}


		public Uuid(byte[] array) {
			_time = BitConverter.ToInt64(array, 0);
			_offset = BitConverter.ToInt64(array, 8);
		}

		static long GetCurrentTimestampMs()
		{
			// unfortunately clock on windows has only millisecond precision :(
			// no nanoseconds
			return (long)(DateTime.UtcNow - Epoch).TotalMilliseconds;
		}

		public static Uuid CreateNew(long offset) {
			return new Uuid(GetCurrentTimestampMs(), offset);
		}
		[Pure]
		public byte[] GetBytes() {
			var result = new byte[16];

			var date = BitConverter.GetBytes(_time);

			var offsetBytes = BitConverter.GetBytes(_offset);

			Array.Copy(date, 0, result, 0, 8);
			Array.Copy(offsetBytes, 0, result, 8, 8);
			return result;
		}
	}



	[TestFixture]
	public sealed class UuidTests {
		[Test]
		public void Roundtrip() {
			var id = Uuid.CreateNew(10);
			
			var bytes = id.GetBytes();
			var restored = new Uuid(bytes);
			Assert.AreEqual(id.GetTimeUtc(), restored.GetTimeUtc());
			Assert.AreEqual(id.GetOffset(), restored.GetOffset());
		} 
	}
}