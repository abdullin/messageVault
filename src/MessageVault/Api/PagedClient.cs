using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using LZ4;
using NUnit.Framework;


namespace MessageVault.Api {

	public sealed class PublishResult {
		public readonly long Position;
		public readonly IList<long> Offsets;

		public PublishResult(long position, IList<long> offsets) {
			Position = position;
			Offsets = offsets;
		}
	}

	public sealed class PagedClient {
		readonly IClient _client;
		readonly string _stream;

		readonly IMemoryStreamManager _manager;


		public int ReadMessagesBuffer = 1000;
		public int ReadBytesBuffer = 2 * 1024 * 1024;

		public PagedClient(IClient client, string stream, IMemoryStreamManager manager = null) {
			_client = client;
			_stream = stream;
			_manager = manager ?? new MemoryStreamFactory();
		}

		public PublishResult Publish(ICollection<UnpackedMessage> unpacked, CancellationToken token) {
			var outgoing = new List<Message>();


			foreach (var message in unpacked) {
				using (var mem = new MemoryStream()) {
					using (var zip = new LZ4Stream(mem, CompressionMode.Compress)) {
						zip.Write(message.Value, 0, message.Value.Length);
					}

					mem.Seek(0, SeekOrigin.Begin);

					var remains = (int) mem.Length;


					while (remains > 0) {
						
						var pick = Math.Min(remains, Constants.MaxValueSize);
						var hasMoreToWrite = remains > pick;

						var flag = hasMoreToWrite ? MessageFlags.ToBeContinued : MessageFlags.None;
						flag |= MessageFlags.LZ4;

						var chunk = new byte[pick];
						mem.Read(chunk, 0, pick);
						outgoing.Add(Message.Create(message.Key, chunk, (byte) flag));
						remains -= pick;
					}
				}
			}
			var result = _client.PostMessagesAsync(_stream, outgoing);
			result.Wait(token);
			return new PublishResult(result.Result.Position, result.Result.Offsets);
		}


		public void ChaseEventsForever(CancellationToken token,
			Action<MessageWithId, Subscription> callback,
			Action<Subscription> idle = null, 
			long start = 0) {
			var reader = _client.GetMessageReaderAsync(_stream);
			reader.Wait(token);


			using (var local = new CancellationTokenSource()) {
				using (var linked = CancellationTokenSource.CreateLinkedTokenSource(token, local.Token)) {
					// TODO - figure buffer size
					var subscription = reader.Result.Subscribe(linked.Token, start, ReadBytesBuffer,
						ReadMessagesBuffer);
					var pages = new List<MessageWithId>();

					while (!token.IsCancellationRequested) {
						MessageWithId msg;
						while (!subscription.Buffer.TryDequeue(out msg)) {
							if (idle != null) {
								idle(subscription);
							}
							if (token.WaitHandle.WaitOne(100)) {
								// time to stop
								return;
							}
						}

						pages.Add(msg);

						var hasMore = ((MessageFlags) msg.Attributes & MessageFlags.ToBeContinued) ==
							MessageFlags.ToBeContinued;
						if (hasMore) {
							continue;
						}


						var total = pages.Sum(m => m.Value.Length);
						using (var mem = _manager.GetStream("chase-1", total)) {
							foreach (var page in pages) {
								mem.Write(page.Value, 0, page.Value.Length);
							}
							mem.Seek(0, SeekOrigin.Begin);

							using (var lz = new LZ4Stream(mem, CompressionMode.Decompress, LZ4StreamFlags.IsolateInnerStream)) {
								using (var output = _manager.GetStream("chase-2")) {
									lz.CopyTo(output);

									var last = pages.Last();

									try {
										callback(new MessageWithId(last.Id, last.Attributes, last.Key, output.ToArray(), 0),
											subscription);
									}
									catch (Exception ex) {
										local.Cancel();
										throw;
									}
								}
							}
						}
						pages.Clear();
					}
				}
			}
		}
	}

}