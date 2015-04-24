using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using MessageVault.Api;
using NUnit.Framework;

namespace MessageVault.Memory {

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
			ICollection<MessageToWrite> messages) {
			var inMem = Get(stream);

			long value;
			lock (inMem.WriteLock) {
				value = inMem.Writer.Append(messages);
			}
			return Task.FromResult(new PostMessagesResponse {
				Position = value
			});
		}

		public Task<MessageReader> GetMessageReaderAsync(string stream) {
			var mem = Get(stream);
			var reader = new MessageReader(mem.Checkpoint, mem.Pages);
			return Task.FromResult(reader);
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
				var task = client.PostMessagesAsync("test", new[] {new MessageToWrite(0, "Key", new byte[0]),});
				var ok = task.Wait(1000);

				Assert.IsTrue(ok);
				Assert.AreEqual(25, task.Result.Position);
			}
		}

		[Test]
		public void Publisher() {
			using (var client = new MemoryClient())
			{
				Publish(client, new MessageToWrite(0, "Key", new byte[0]));
			}
		}



		public async Task PublishAsync(IClient client, params MessageToWrite[] messages)
		{
			await  client.PostMessagesAsync("demo", messages);
		}

		public void Publish(IClient client, params MessageToWrite[] events)
		{
			PublishAsync(client, events).Wait();
		}
	}

}