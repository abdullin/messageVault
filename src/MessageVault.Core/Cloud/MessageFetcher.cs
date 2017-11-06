using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MessageVault.MemoryPool;

namespace MessageVault.Cloud {

	public sealed class MessageFetcher 
	{
		readonly IPageReader _remote;
		readonly ICheckpointReader _remotePos;
		readonly IMemoryStreamManager _streamManager;
		public readonly string StreamName;
		

		public MessageFetcher(IPageReader remote, ICheckpointReader remotePos, IMemoryStreamManager streamManager, string streamName) {
			_remote = remote;
			_remotePos = remotePos;
			_streamManager = streamManager;
			StreamName = streamName;
		}

		static bool TryRead(BinaryReader reader, out MessageWithId msg)
		{
			try
			{
				msg = StorageFormat.Read(reader);
				return true;
			}
			catch (EndOfStreamException)
			{
				msg = null;
				return false;
			}
		}

		public int AmountToLoadMax = 4 * 1024 * 1024;


		public async Task<DirectReadBulkResult> ReadAll(CancellationToken token, long startingFrom, int maxCount) {
			
			var result = new DirectReadBulkResult();
			var stats = await ReadAll(token, startingFrom, maxCount, (id, position, maxPosition) => {
				if (result.Messages == null)
				{
					result.Messages = new List<MessageHandlerClosure>();
				}
				result.Messages.Add(new MessageHandlerClosure
				{
					CurrentCachePosition = position,
					MaxCachePosition = maxPosition,
					Message = id
				});
			}).ConfigureAwait(false);

			result.CurrentPosition = stats.CurrentPosition;
			result.MaxPosition = stats.MaxPosition;
			result.ReadRecords = stats.ReadRecords;
			result.StartingPosition = stats.StartingPosition;
			result.Elapsed = stats.Elapsed;
			return result;
		}

		public async Task<DirectReadResult> ReadAll(CancellationToken token, long startingFrom, int maxCount, MessageHandler handler)
		{
			//var convertedLocalPos = pos[0];
			//var currentRemotePosition = pos[1];

			var watch = Stopwatch.StartNew();
			var maxPos = await _remotePos.ReadAsync(token)
				.ConfigureAwait(false);
			var result = new DirectReadResult
			{
				StartingPosition = startingFrom,
				CurrentPosition = startingFrom,
				MaxPosition = maxPos,
			};

			if (maxPos <= startingFrom)
			{
				// we don't have anything to write
				return result;
			}

			var availableAmount = maxPos - startingFrom;
			if (availableAmount <= 0)
			{
				// we don't have anything to write
				return result;
			}

			var amountToLoad = Math.Min(availableAmount, AmountToLoadMax);

			using (var mem = _streamManager.GetStream("fetcher"))
			{
				await _remote.DownloadRangeToStreamAsync(mem, startingFrom, (int)amountToLoad)
					.ConfigureAwait(false);
				// cool, we've got some data back

				mem.Seek(0, SeekOrigin.Begin);

				var usedBytes = 0L;

				var pages = new List<MessageWithId>();
				using (var bin = new BinaryReader(mem))
				{
					MessageWithId msg;
					while (TryRead(bin, out msg))
					{
						var hasMorePagesToRead = HasMore(msg);

						if (false == hasMorePagesToRead && pages.Count == 0)
						{
							// fast path, we can save message directly without merging
							handler(msg,msg.Id.GetOffset(),maxPos);

							//WriteMessage(msg);
							usedBytes = mem.Position;
							result.ReadRecords += 1;
							
							continue;
						}

						pages.Add(msg);
						if (hasMorePagesToRead)
						{
							continue;
						}

						var total = pages.Sum(m => m.Value.Length);
						using (var sub = _streamManager.GetStream("chase-1", total))
						{
							foreach (var page in pages)
							{
								sub.Write(page.Value, 0, page.Value.Length);
							}
							sub.Seek(0, SeekOrigin.Begin);
							var last = pages.Last();
							var final = new MessageWithId(last.Id, last.Attributes, last.Key, sub.ToArray(), 0);
							handler(final, last.Id.GetOffset(), maxPos);
							usedBytes = mem.Position;
							result.ReadRecords += 1;
							
						}
						pages.Clear();

					}

				}
				result.CurrentPosition = result.StartingPosition + usedBytes;
				result.Elapsed = watch.Elapsed;
				return result;
			}
		}


		static bool HasMore(MessageWithId msg)
		{
			var hasMore = ((MessageFlags)msg.Attributes & MessageFlags.ToBeContinued) ==
			              MessageFlags.ToBeContinued;
			return hasMore;
		}
	}

}