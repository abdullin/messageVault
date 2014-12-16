using System;
using System.Diagnostics.Contracts;
using System.Text;
using System.Threading;

namespace MessageVault {

	/// <summary>
	///   <para>
	///     Binary-sortable 16-byte MessageId with a custom scheme (big-endian)
	///     [timestamp:8] - [stream-offset:8]. You can use this as key in FoundationDB
	/// or as uuid in PostgreSQL with predictable effects.
	///   </para>
	///   <para>
	///     MessageIds generated within a single process are unique and sortable. MessageIds
	/// generated in different processes are sortable (within time drift) and are very likely 
	/// to be unique (since we embedd offset).
	///   </para>
	/// </summary>
	/// <remarks>
	///   http://en.wikipedia.org/wiki/Endianness
	/// </remarks>
	public struct MessageId : IComparable<MessageId>, IComparable {
		public static readonly DateTime Epoch =
			new DateTime(2010, 1, 1, 0, 0, 0, DateTimeKind.Utc);


		static int _counter = GetNodeLocalNumber();

		static int GetNodeLocalNumber() {
			var guid = Guid.NewGuid().ToByteArray();

			// MS uses native GUID generator where last 8 bytes are node-local;
			// use that as a seed
			var seed = BitConverter.ToInt32(guid, 12) ^ BitConverter.ToInt32(guid, 8);
			// shift one bit, since we don't want counter to overflow right away
			return seed >> 1;
		}

		readonly int _a;
		readonly int _b;
		readonly int _c;
		readonly int _d;


		[Pure]
		public DateTime GetTimeUtc() {
			long offset = ((long) _a << 16) + (_b >> 16);
			return Epoch.AddMilliseconds(offset);
		}

		[Pure]
		public long GetOffset() {
			return ((long) _b << 16) + _c;
		}

		[Pure]
		public int GetRand() {
			return _d;
		}

		[Pure]
		public bool IsEmpty() {
			return _a == 0 & _b == 0 & _c == 0 & _d == 0;
		}

		public MessageId(int a, int b, int c, int d) {
			_a = a;
			_b = b;
			_c = c;
			_d = d;
		}

		internal static int ReadIntInBigEndian(byte[] array, int position) {
			return (array[position + 0] << 24) +
				(array[position + 1] << 16) +
				(array[position + 2] << 8) +
				(array[position + 3]);
		}

		internal static void WriteIntInBigEndian(int value, byte[] array, int position) {
			array[position + 0] = (byte) ((value & 0xFF000000) >> 24);
			array[position + 1] = (byte) ((value & 0xFF0000) >> 16);
			array[position + 2] = (byte) ((value & 0xFF00) >> 8);
			array[position + 3] = (byte) ((value & 0xFF));
		}

		public static readonly MessageId Empty = default(MessageId);


		public MessageId(byte[] array) {
			_a = ReadIntInBigEndian(array, 0);
			_b = ReadIntInBigEndian(array, 4);
			_c = ReadIntInBigEndian(array, 8);
			_d = ReadIntInBigEndian(array, 12);
		}

		public override string ToString() {
			var builder = new StringBuilder(32 + 3);
			builder.Append(_a.ToString("x8"));
			builder.Append(_b.ToString("x8"));
			builder.Append(_c.ToString("x8"));
			builder.Append(_d.ToString("x8"));

			builder.Insert(6 * 4, '-');
			builder.Insert(6 * 2, '-');

			return builder.ToString();
		}

		static long GetCurrentTimestampMs() {
			// unfortunately clock on windows has only millisecond precision :(
			// no nanoseconds
			return (long) (DateTime.UtcNow - Epoch).TotalMilliseconds;
		}

		public static MessageId CreateNew(long offset) {
			var counter = Interlocked.Increment(ref _counter);
			var timestamp = GetCurrentTimestampMs();
			// aaaaaaaabbbbbbbbccccccccdddddddd
			// timestamp...offset......rand....
			// AABBCCDDEEFF00112233445566778899
			// 128 bits
			// 16 bytes
			if (offset > 0xFFFFFFFFFFFF) {
				throw new ArgumentOutOfRangeException("offset", "Offset must fit in 6 bytes");
			}

			var a = timestamp >> 16;
			var b = ((timestamp & 0xFFFF) << 16) + (offset >> 24);
			var c = offset & 0xFFFFFF;
			var d = counter;
			return new MessageId((int) a, (int) b, (int) c, d);
		}

		[Pure]
		public byte[] GetBytes() {
			var result = new byte[16];
			WriteIntInBigEndian(_a, result, 0);
			WriteIntInBigEndian(_b, result, 4);
			WriteIntInBigEndian(_c, result, 8);
			WriteIntInBigEndian(_d, result, 12);

			return result;
		}

		public int CompareTo(MessageId other) {
			var a = _a.CompareTo(other._a);
			if (a != 0) {
				return a;
			}
			var b = _b.CompareTo(other._b);
			if (b != 0) {
				return b;
			}
			var c = _c.CompareTo(other._c);
			if (c != 0) {
				return c;
			}
			return _d.CompareTo(other._d);
		}


		public override int GetHashCode() {
			unchecked {
				var hashCode = _b;
				hashCode = (hashCode * 397) ^ _a;
				hashCode = (hashCode * 397) ^ _c;
				hashCode = (hashCode * 397) ^ _d;
				return hashCode;
			}
		}

		public int CompareTo(object obj) {
			if (obj == null) {
				return 1;
			}
			if (obj is MessageId) {
				var value = (MessageId) obj;
				return CompareTo(value);
			}
			throw new InvalidOperationException("Can't compare with non-MessageId");
		}
	}

}