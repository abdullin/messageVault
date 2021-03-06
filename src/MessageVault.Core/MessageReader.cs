using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace MessageVault {

	public sealed class Subscription {
		public readonly ConcurrentQueue<MessageWithId> Buffer = new ConcurrentQueue<MessageWithId>();
		public Task Task { get; internal set; }

		public long DebugStartPosition { get; internal set; }
		public long DebugKnownMaxOffset { get; internal set; }
		public long DebugEnqueuedOffset { get; internal set; }
	}


	

	public sealed class MessageReader : IDisposable {
		public readonly ICheckpointReader Position;
		public readonly IPageReader Messages;

		readonly byte[] _buffer;
		const int Limit = 1024 * 1024 * 4;

		public MessageReader(ICheckpointReader position, IPageReader messages) {
			Position = position;
			Messages = messages;
			_buffer = new byte[Limit];
		}


		public long GetPosition() {
			return Position.Read();
		}

		public void ReadMessages(long from, long till, int maxCount, Action<MessageWithId> action)
		{
			Require.ZeroOrGreater("from", from);
			Require.ZeroOrGreater("maxOffset", till);
			Require.Positive("maxCount", maxCount);
			
			var count = 0;
			using (var prs = new PageReadStream(Messages, from, till, _buffer))
			{
				using (var bin = new BinaryReader(prs))
				{
					while (prs.Position < prs.Length)
					{
						
						var message = StorageFormat.Read(bin);
						action(message);
						count += 1;
						if (count >= maxCount)
						{
							break;
						}
					}
				}
			}
		}

		public MessageResult ReadMessages(long from, long till, int maxCount) {
			Require.ZeroOrGreater("from", from);
			Require.ZeroOrGreater("maxOffset", till);
			Require.Positive("maxCount", maxCount);

			var list = new List<MessageWithId>(maxCount);
			var position = from;

			using (var prs = new PageReadStream(Messages, from, till, _buffer)) {
				using (var bin = new BinaryReader(prs)) {
					while (prs.Position < prs.Length) {
						var message = StorageFormat.Read(bin);
						list.Add(message);
						position = prs.Position;
						if (list.Count >= maxCount) {
							break;
						}
					}
				}
			}
			return new MessageResult(list, position);
		}

		public Subscription Subscribe(
			CancellationToken ct,
			long start,
			int bufferSize,
			int cacheSize
			) {
			var sub = new Subscription();

			sub.Task = Task.Factory.StartNew(() => RunSubscription(sub, start, ct, bufferSize, cacheSize),
				TaskCreationOptions.LongRunning);
			return sub;
		}


		void RunSubscription(
			Subscription sub,
			long position,
			CancellationToken ct,
			int bufferSize,
			int cacheSize
			) {

			sub.DebugStartPosition = position;
			var buffer = new byte[bufferSize];
			// forever try
			while (!ct.IsCancellationRequested) {
				try {
					// read current max length
					var length = Position.Read();
					sub.DebugKnownMaxOffset = length;
					using (var prs = new PageReadStream(Messages, position, length, buffer)) {
						using (var bin = new BinaryReader(prs)) {
							while (prs.Position < prs.Length) {
								var message = StorageFormat.Read(bin);
								sub.Buffer.Enqueue(message);
								sub.DebugEnqueuedOffset = prs.Position;
								position = prs.Position;

								while (sub.Buffer.Count >= cacheSize) {
									ct.WaitHandle.WaitOne(500);
								}
							}
						}
					}
					// wait till we get chance to advance
					while (Position.Read() == position) {
						if (ct.WaitHandle.WaitOne(1000)) {
							return;
						}
					}
				}
				catch (ForbiddenException) {
					throw;
				}
				catch (Exception ex) {
					Debug.Print("Exception {0}", ex);
					ct.WaitHandle.WaitOne(1000 * 5);
				}
			}
		}



		public async Task<MessageResult> GetMessagesAsync(CancellationToken ct, long start,
			int limit) {
			while (!ct.IsCancellationRequested) {
				var actual = Position.Read();
				if (actual < start) {
					var msg = string.Format("Actual stream length is {0}, but requested {1}", actual,
						start);
					throw new InvalidOperationException(msg);
				}
				if (actual == start) {
					await Task.Delay(1000, ct);
					continue;
				}
				var result = await Task.Run(() => ReadMessages(start, actual, limit), ct);

				return result;
			}
			return MessageResult.Empty(start);
		}

		bool _disposed;

		public void Dispose() {
			if (_disposed) {
				return;
			}
			using (Messages) {
				using (Position) {
					_disposed = true;
				}
			}
		}
	}

}