using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using MessageVault.Api;
using MessageVault.Cloud;
using MessageVault.Files;
using MessageVault.MemoryPool;

namespace MessageVault {

	public sealed class FileClient : IClient 
	{
		public sealed class Locked {
			
			public readonly MessageWriter Writer;

			internal readonly object WriteLock = new object();
			public Locked(MessageWriter writer) {
				Writer = writer;
			}
		}

		readonly ConcurrentDictionary<string,Locked> _writers = new ConcurrentDictionary<string, Locked>();
		readonly DirectoryInfo _dir;

		public FileClient(string folder) {
			_dir = new DirectoryInfo(folder);
			if (!_dir.Exists) {
				_dir.Create();
			}
		}

		public void Dispose() {
			foreach (var writer in _writers) {
				writer.Value.Writer.Dispose();
			}
		}

		public Task<PostMessagesResponse> PostMessagesAsync(string stream, ICollection<Message> messages) {

			var writer = _writers.GetOrAdd(stream, s => new Locked(FileSetup.CreateAndInitWriter(_dir, stream)));
			AppendResult result;
			lock (writer.WriteLock) {
				result = writer.Writer.Append(messages);
			}
			var response = PostMessagesResponse.FromAppendResult(result);
			return Task.FromResult(response);
		}



		public Task<MessageReader> GetMessageReaderAsync(string stream) {
			return Task.FromResult(GetMessageReader(stream));
		}

		public MessageReader GetMessageReader(string stream) {
			return FileSetup.GetReader(_dir, stream);
		}

		public MessageFetcher GetFetcher(string stream, IMemoryStreamManager manager = null) {
			if (stream == null) throw new ArgumentNullException(nameof(stream));
			var raw = FileSetup.GetReaderRaw(_dir, stream);

			return new MessageFetcher(raw.Item2, raw.Item1, manager ?? MemoryStreamFactoryManager.Instance, stream);
		}
	}

}