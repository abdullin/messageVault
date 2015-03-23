using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using MessageVault.Api;

namespace MessageVault.Memory {

	public sealed class MemoryClient : IClient {

		sealed class InMemStream {
			public readonly MemoryPageReaderWriter Pages;
			public readonly MemoryCheckpointReaderWriter Checkpoint;
			public readonly MessageWriter Writer;

			public InMemStream() {
				Pages = new MemoryPageReaderWriter();
				Pages.Init();
				Checkpoint = new MemoryCheckpointReaderWriter();
				Writer = new MessageWriter(Pages, Checkpoint);
			}
		}

		readonly ConcurrentDictionary<string, InMemStream> _streams = new ConcurrentDictionary<string, InMemStream>(StringComparer.InvariantCultureIgnoreCase);

		InMemStream Get(string name) {
			return _streams.GetOrAdd(name, s => new InMemStream());
		}

		public Task<PostMessagesResponse> PostMessagesAsync(string stream, ICollection<MessageToWrite> messages) {
			var inMem = Get(stream);

			// we need to ensure only a single writer
			return Task.Factory.StartNew(() => {
				lock (inMem.Writer) {
					var value = inMem.Writer.Append(messages);
					return new PostMessagesResponse {
						Position = value
					};
				}
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

}