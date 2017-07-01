using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using MessageVault.Api;
using MessageVault.Cloud;
using MessageVault.Memory;
using MessageVault.MemoryPool;
using NUnit.Framework;

namespace MessageVault {

	public sealed class MemoryClient : IClient {
		sealed class InMemStream {
			public readonly MemoryPageReaderWriter Pages;
			public readonly MemoryCheckpointReaderWriter Checkpoint;
			public readonly MessageWriter Writer;

			internal readonly object WriteLock = new object();

			public InMemStream() {
				Pages = new MemoryPageReaderWriter();
				Pages.Init();
				Checkpoint = new MemoryCheckpointReaderWriter();
				Writer = new MessageWriter(Pages, Checkpoint);
			}
		}


		readonly ConcurrentDictionary<string, InMemStream> _streams =
			new ConcurrentDictionary<string, InMemStream>(StringComparer.InvariantCultureIgnoreCase);

		InMemStream Get(string name) {
			return _streams.GetOrAdd(name, s => new InMemStream());
		}

		public Task<PostMessagesResponse> PostMessagesAsync(string stream,
			ICollection<Message> messages) {
			var inMem = Get(stream);

			AppendResult value;
			lock (inMem.WriteLock) {
				value = inMem.Writer.Append(messages);
			}
			var response = PostMessagesResponse.FromAppendResult(value);
			return Task.FromResult(response);
		}

		public Task<MessageReader> GetMessageReaderAsync(string stream) {
			return Task.FromResult(GetMessageReader(stream));
		}

		public MessageReader GetMessageReader(string stream) {
			var mem = Get(stream);
			return new MessageReader(mem.Checkpoint, mem.Pages);
		}

		public MessageFetcher GetFetcher(string stream, IMemoryStreamManager manager) {
			var mem = Get(stream);
			return new MessageFetcher(mem.Pages, mem.Checkpoint, manager ?? MemoryStreamFactoryManager.Instance, stream);
		}

		public void Dispose() {
			_streams.Clear();
		}
	}

	[TestFixture]
	public sealed class MemoryClientTests {
		[Test]
		public void Posting() {
			using (var client = new MemoryClient()) {
				var task = client.PostMessagesAsync("test", new[] {Message.Create("Key", new byte[0]),});
				var ok = task.Wait(1000);

				Assert.IsTrue(ok);
				Assert.AreEqual(25, task.Result.Position);
			}
		}

		[Test]
		public void Publisher() {
			using (var client = new MemoryClient())
			{
				Publish(client, Message.Create("Key", new byte[0]));
			}
		}



		public async Task PublishAsync(IClient client, params Message[] messages)
		{
			await  client.PostMessagesAsync("demo", messages);
		}

		public void Publish(IClient client, params Message[] events)
		{
			PublishAsync(client, events).Wait();
		}
	}

}