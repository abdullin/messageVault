using System;
using System.Diagnostics.Contracts;
using System.Text;
using System.Threading;

namespace MessageVault {

	/// <summary>
	///   <para>
	///     Binary-sortable 16-byte MessageId with a custom scheme
	///     [timestamp:6] - [stream-offset:6] - [rand:4].
	///     You can use this as key in FoundationDB
	///     or as uuid in PostgreSQL with predictable effects. It includes generation time in UTC,
	///     message offset and a node-local incrementing number.
	///   </para>
	///   <para>
	///     MessageIds generated within a single process are unique and sortable. MessageIds
	///     generated in different processes are sortable (within time drift) and are very likely
	///     to be unique (since we embedd offset).
	///   </para>
	///   <para>
	///     Time can run to 2044, offset can run to 255TB (a different format is needed if these are exceeded). Random part
	///     is seeded with node-local id (taken from Guid) and incremented by 1 for each ID.
	///   </para>
	/// </summary>
	/// <remarks>
	///   Time, offset and rand are stored as Big-Endian
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

		readonly uint _a;
		readonly uint _b;
		readonly uint _c;
		readonly uint _d;


		[Pure]
		public DateTime GetTimeUtc() {
			long offset = ((long) _a << 16) + (_b >> 16);
			return Epoch.AddMilliseconds(offset);
		}

		[Pure]
		public long GetOffset() {

			var offset = (long)_c +(((long)_b & 0xFFFF) << 32);
			Ensure.ZeroOrGreater("offset", offset);
			return offset;
		}

		[Pure]
		public int GetRand() {
			return (int)_d;
		}

		[Pure]
		public bool IsEmpty() {
			return _a == 0 & _b == 0 & _c == 0 & _d == 0;
		}

		public MessageId(uint a, uint b, uint c, uint d) {
			_a = a;
			_b = b;
			_c = c;
			_d = d;
		}

		internal static uint ReadUintInBigEndian(byte[] array, int position) {
			return ((uint)array[position + 0] << 24) +
				((uint)array[position + 1] << 16) +
				((uint)array[position + 2] << 8) +
				((uint)array[position + 3]);
		}

		internal static void WriteIntInBigEndian(uint value, byte[] array, int position) {
			array[position + 0] = (byte) ((value & 0xFF000000) >> 24);
			array[position + 1] = (byte) ((value & 0xFF0000) >> 16);
			array[position + 2] = (byte) ((value & 0xFF00) >> 8);
			array[position + 3] = (byte) ((value & 0xFF));
		}

		public static readonly MessageId Empty = default(MessageId);

		public MessageId(Guid id): this(id.ToByteArray()) {}

		public MessageId(byte[] array) {
			_a = ReadUintInBigEndian(array, 0);
			_b = ReadUintInBigEndian(array, 4);
			_c = ReadUintInBigEndian(array, 8);
			_d = ReadUintInBigEndian(array, 12);
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
			return new MessageId(timestamp, offset, counter);
		}

		public MessageId(long timestamp, long offset, int counter) {
			Require.ZeroOrGreater("offset", offset);
			if (offset > 0xFFFFFFFFFFFF)
			{
				throw new ArgumentOutOfRangeException("offset", "Offset must fit in 6 bytes");
			}


			// aaaaaaaabbbbbbbbccccccccdddddddd
			// timestamp...offset......rand....
			// AABBCCDDEEFF00112233445566778899
			// 128 bits
			// 16 bytes

			_a = (uint) (timestamp >> 16);
			_b = (uint) (((timestamp & 0xFFFF) << 16) + (offset >> 32));
			_c = (uint) (offset & 0xFFFFFFFF);
			_d = (uint) counter;
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
		[Pure]
		public Guid ToGuid() {
			return new Guid(GetBytes());
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
				return (int) hashCode;
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