using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using LZ4n;

namespace MessageVault.Api {

	/// <summary>
	/// Replace with RecyclableMemoryStream from Microsoft, if you want to use your pool
	/// </summary>
	public interface IMemoryStreamManager {
		MemoryStream GetStream(string tag);
		MemoryStream GetStream(string tag, int length);
	}

	public sealed class MemoryStreamFactory : IMemoryStreamManager {
		public MemoryStream GetStream(string tag) {
			return new MemoryStream();
		}

		public MemoryStream GetStream(string tag, int length) {
			return new MemoryStream();
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

		public  long Publish(ICollection<UnpackedMessage> unpacked, CancellationToken token)
		{
			var outgoing = new List<Message>();


			foreach (var message in unpacked) {
				using (var mem = new MemoryStream()) {
					using (var zip = new LZ4Stream(mem, CompressionMode.Compress, false, 0, true)) {
						zip.Write(message.Value, 0, message.Value.Length);
					}

					mem.Seek(0, SeekOrigin.Begin);

					var remains = (int)mem.Length;
					

					while (remains > 0)
					{
						Console.WriteLine("Page...");
						var pick = Math.Min(remains, Constants.MaxValueSize);
						var hasMoreToWrite = remains > pick;

						var flag = hasMoreToWrite ? MessageFlags.ToBeContinued : MessageFlags.None;
						flag |= MessageFlags.LZ4;

						var chunk = new byte[pick];
						mem.Read(chunk, 0, pick);
						outgoing.Add(Message.Create(message.Key, chunk, (byte)flag));
						remains -= pick;
					}
				}
			}
			var result = _client.PostMessagesAsync(_stream, outgoing);
			result.Wait(token);
			return result.Result.Position;
		}


		public void ChaseEventsForever(CancellationToken token, 
			Action<MessageWithId, Subscription> callback,
			Action<Subscription> idle = null)
		{
			const int current = 0;
			var reader =  _client.GetMessageReaderAsync(_stream);
			reader.Wait(token);


			using (var local = new CancellationTokenSource())
			using (var linked = CancellationTokenSource.CreateLinkedTokenSource(token, local.Token)){
			
			// TODO - figure buffer size
			var subscription = reader.Result.Subscribe(linked.Token, current, ReadBytesBuffer, ReadMessagesBuffer);
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

						using (var lz = new LZ4Stream(mem, CompressionMode.Decompress, keepOpen : true)) {
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