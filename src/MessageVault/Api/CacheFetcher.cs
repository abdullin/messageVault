using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MessageVault.Cloud;
using MessageVault.Files;

namespace MessageVault.Api {

	/// <summary>
	/// Fetches multiple caches in parallel
	/// </summary>
	public sealed class CacheManager {
		readonly IDictionary<string, string> _nameAndSas;
		readonly DirectoryInfo _cacheFolder;
		readonly IMemoryStreamManager _manager;


		public CacheManager(IDictionary<string, string> nameAndSas, DirectoryInfo cacheFolder,
			IMemoryStreamManager manager) {
			_nameAndSas = nameAndSas;
			_cacheFolder = cacheFolder;
			_manager = manager;
		}

		public void Run(CancellationToken token) {

			var fetchers = _nameAndSas
				.Select(p => {
					var fetcher = new CacheFetcher(p.Value, p.Key, _cacheFolder, _manager);
					fetcher.Init();
					return fetcher;
				})
				.ToArray();


			var array = new Task<FetchResult>[fetchers.Length];

			
			
			while (!token.IsCancellationRequested) {
				
				for (int i = 0; i < fetchers.Length; i++) {
					array[i] = fetchers[i].DownloadNext();
					array[i].Start();
				}
				
				Task.WaitAll(array, token);
				var downloaded = array.Sum(t => t.Result.DownloadedBytes);
				if (downloaded == 0) {
					// no activity
					token.WaitHandle.WaitOne(TimeSpan.FromSeconds(1));
				}
			}
		}
	}


	public sealed class FetchResult {
		public long CachedRemotePosition;
		public long ActualRemotePosition;
		public long LocalStoragePosition;
		public long DownloadedBytes;
		public long UsedBytes;
		public long SavedBytes;
	}

	public sealed class CacheFetcher {
		readonly CloudPageReader _remote;
		readonly CloudCheckpointReader _remotePos;

		readonly FileStream _output;
		readonly FileCheckpointArrayWriter _outputPos;

		readonly IMemoryStreamManager _streamManager;
		readonly BinaryWriter _writer;

		public static CacheFetcher CreateStandalone(string sas, string stream, DirectoryInfo folder) {
			var fetcher = new CacheFetcher(sas, stream, folder, new MemoryStreamFactory());
			fetcher.Init();
			return fetcher;
		}

		public CacheFetcher(string sas, string stream, DirectoryInfo folder, IMemoryStreamManager streamManager) {
			_streamManager = streamManager;
			var raw = CloudSetup.GetReaderRaw(sas);

			_remote = raw.Item2;
			_remotePos = raw.Item1;

			var streamDir = Path.Combine(folder.FullName, stream);
			var di = new DirectoryInfo(streamDir);
			if (!di.Exists)
			{
				di.Create();
			}
			var outputInfo = new FileInfo(Path.Combine(di.FullName, Constants.CacheStreamName));
			var checkpointFile = new FileInfo(Path.Combine(di.FullName, Constants.CachePositionName));

			outputInfo.Refresh();
			_output = outputInfo.Open(outputInfo.Exists ? FileMode.Open : FileMode.Create, FileAccess.ReadWrite, FileShare.Read);
			_writer = new BinaryWriter(_output);
			_outputPos = new FileCheckpointArrayWriter(checkpointFile, 2);
		}

		static bool TryRead(BinaryReader reader, out MessageWithId msg) {
			try {
				msg = StorageFormat.Read(reader);
				return true;
			}
			catch (EndOfStreamException) {
				msg = null;
				return false;
			}
		}

		public int AmountToLoadMax = 1*1024*1024;

		public void Init() {
			_outputPos.GetOrInitPosition();
		}

		public async Task<FetchResult> DownloadNext() {
			var pos = _outputPos.ReadPositionVolatile();

			var convertedLocalPos = pos[0];
			var cachedRemotePos = pos[1];
			

			var actualRemotePos = _remotePos.Read();
			var result = new FetchResult() {
				CachedRemotePosition = cachedRemotePos,
				ActualRemotePosition = actualRemotePos,
				LocalStoragePosition = convertedLocalPos,
				DownloadedBytes = 0,
				UsedBytes = 0,
				SavedBytes = 0
			};

			if (actualRemotePos <= cachedRemotePos) {
				// we don't have anything to write
				return result;
			}

			var availableAmount = actualRemotePos - cachedRemotePos;
			var amountToLoad = Math.Min(availableAmount, AmountToLoadMax);
			
			long usedBytes = 0;

			using (var mem = _streamManager.GetStream("fetcher")) {
				await _remote.DownloadRangeToStreamAsync(mem, cachedRemotePos, (int) amountToLoad)
					.ConfigureAwait(false);
				// cool, we've got some data back

				result.DownloadedBytes = mem.Position;
				mem.Seek(0, SeekOrigin.Begin);


				_output.Seek(convertedLocalPos, SeekOrigin.Begin);

				var pages = new List<MessageWithId>();
				using (var bin = new BinaryReader(mem)) {
					MessageWithId msg;
					while (TryRead(bin, out msg)) {
						var hasMorePagesToRead = HasMore(msg);

						if (false == hasMorePagesToRead && pages.Count == 0) {
							// fast path, we can save message directly without merging
							WriteMessage(msg);
							usedBytes = mem.Position;
							continue;
						}

						pages.Add(msg);
						if (hasMorePagesToRead){
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
							WriteMessage(final);
							usedBytes = mem.Position;
						}
						pages.Clear();

					}
				}
				if (usedBytes == 0) {
					return result;
				}

				_output.Flush(true);
				_outputPos.Update(new[] {
					_output.Position,
					cachedRemotePos + usedBytes,
				});

				result.UsedBytes = usedBytes;
				result.SavedBytes = _output.Position - result.LocalStoragePosition;
				return result;
			}
		}

		void WriteMessage(MessageWithId msg) {
			EnsureSizeToAvoidFragmentation();
			StorageFormat.Write(_writer, msg);
		}

		void EnsureSizeToAvoidFragmentation()
		{
			const long shouldHaveMb = 2*1024*1024;
			const long increaseByMb = 5*shouldHaveMb;
			var current = _output.Length;
			var pos = _output.Position;

			var available = current - pos;
			if (available < shouldHaveMb) {
				_output.SetLength(_output.Length + increaseByMb);
			}


		}

		static bool HasMore(MessageWithId msg) {
			var hasMore = ((MessageFlags) msg.Attributes & MessageFlags.ToBeContinued) ==
			              MessageFlags.ToBeContinued;
			return hasMore;
		}
	}

}