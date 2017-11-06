using System;
using System.Collections.Generic;

namespace MessageVault {

	[Flags]
	public enum MessageFlags : byte
	{
		None = 0,
		LZ4 = 0x01,
		ToBeContinued = 0x02,
	}

	public sealed class ReadResult {
		public int ReadRecords;
		public long CurrentCachePosition;
		public long StartingCachePosition;
		public long AvailableCachePosition;
		public long MaxOriginPosition;
		public long CachedOriginPosition;
		public bool ReadEndOfCacheBeforeItWasFlushed;
	}

	public sealed class DirectReadResult
	{
		public int ReadRecords;
		public long CurrentPosition;
		public long StartingPosition;
		public long MaxPosition;
		public TimeSpan Elapsed;
	}
	public sealed class DirectReadBulkResult
	{
		public int ReadRecords;
		public long CurrentPosition;
		public long StartingPosition;
		public long MaxPosition;
		public TimeSpan Elapsed;
		public IList<MessageHandlerClosure> Messages;

	}
	public delegate void MessageHandler(MessageWithId id, long currentPosition, long maxPosition);

	public sealed class MessageHandlerClosure
	{
		public MessageWithId Message;
		public long CurrentCachePosition;
		public long MaxCachePosition;
	}

	public sealed class ReadBulkResult
	{
		public int ReadRecords;
		public long CurrentCachePosition;
		public long StartingCachePosition;
		public long AvailableCachePosition;
		public long MaxOriginPosition;
		public long CachedOriginPosition;
		public bool ReadEndOfCacheBeforeItWasFlushed;
		public IList<MessageHandlerClosure> Messages;
	}
}