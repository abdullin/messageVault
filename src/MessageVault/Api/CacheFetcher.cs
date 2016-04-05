using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MessageVault.Cloud;
using MessageVault.Files;
using MessageVault.MemoryPool;

namespace MessageVault.Api {

	/// <summary>
	/// Fetches multiple caches in parallel
	/// </summary>
	public sealed class CacheManager {
		readonly DirectoryInfo _cacheFolder;
		readonly IMemoryStreamManager _manager;
		readonly CacheFetcher[] _fetchers;

		public CacheManager(IDictionary<string, string> nameAndSas, DirectoryInfo cacheFolder,
			IMemoryStreamManager manager) {
			_cacheFolder = cacheFolder;
			_manager = manager;

			_fetchers = nameAndSas
				.Select(p =>
				{
					var fetcher = new CacheFetcher(p.Value, p.Key, _cacheFolder, _manager);
					fetcher.Init();
					return fetcher;
				})
				.ToArray();
		}

		public IList<CacheFetcher> GetFetchers() {
			return _fetchers;
		} 

		public void Run(CancellationToken token) {

			var array = new Task<FetchResult>[_fetchers.Length];

			
			
			while (!token.IsCancellationRequested) {
				
				for (int i = 0; i < _fetchers.Length; i++) {
					array[i] = _fetchers[i].DownloadNext();
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

	public delegate void MessageHandler(MessageWithId id, long currentPosition, long maxPosition);

	public sealed class CacheReader {
		readonly FileCheckpointArrayWriter _checkpoint;
		readonly FileCheckpointArrayReader _checkpointReader;
		readonly FileStream _readStream;
		readonly BinaryReader _reader;

		public CacheReader(FileCheckpointArrayWriter checkpoint, FileStream readStream, FileCheckpointArrayReader checkpointReader) {
			_checkpoint = checkpoint;
			_readStream = readStream;
			_checkpointReader = checkpointReader;

			_reader = new BinaryReader(readStream, new UTF8Encoding(false));
		}

		public ReadBulkResult ReadAll(long startingFrom, int maxCount) {

			var result = new ReadBulkResult();
			var stats = ReadAll(startingFrom, maxCount, (id, position, maxPosition) => {
				if (result.Messages == null) {
					result.Messages = new List<MessageHandlerClosure>();
				}
				result.Messages.Add(new MessageHandlerClosure {
					CurrentCachePosition = position,
					MaxCachePosition = maxPosition,
					Message = id
				});
			});

			result.AvailableCachePosition = stats.AvailableCachePosition;
			result.CurrentCachePosition = stats.CurrentCachePosition;
			result.ReadEndOfCacheBeforeItWasFlushed = stats.ReadEndOfCacheBeforeItWasFlushed;
			result.ReadRecords = stats.ReadRecords;
			result.StartingCachePosition = stats.StartingCachePosition;
			return result;
		}

		public ReadResult ReadAll(long startingFrom, int maxCount, MessageHandler handler) {
			var maxPos = _checkpoint.ReadPositionVolatile()[0];

			var result = new ReadResult() {
				StartingCachePosition = startingFrom,
				AvailableCachePosition = maxPos,
				CurrentCachePosition = startingFrom
			};
			if (startingFrom >= maxPos) {
				return result;
			}


			// double check on the file
			maxPos = _checkpointReader.Read()[0];
			if (startingFrom >= maxPos) {
				return result;
			}
			
			_readStream.Seek(startingFrom, SeekOrigin.Begin);
			long currentPosition = startingFrom;
			try {
				for (int i = 0; i < maxCount; i++) {

					currentPosition = _readStream.Position;
					if (currentPosition >= maxPos) {
						break;
					}
					var frame = CacheStorage.Read(_reader);


					handler(frame, currentPosition, maxPos);
					// fix the position
					result.ReadRecords += 1;
					result.CurrentCachePosition = _readStream.Position;
				}
			}
			catch (InvalidStorageFormatException ex) {
				throw new InvalidOperationException("Unexpected header at " + currentPosition, ex);
			}
			catch (NoDataException) {
				// not a problem, we just read end of cache before it was flushed to disk
				result.ReadEndOfCacheBeforeItWasFlushed = true;
			}
			catch (EndOfStreamException) {
				result.ReadEndOfCacheBeforeItWasFlushed = true;
			}

			return result;


		}


	}

	public sealed class MessageHandlerClosure {
		public  MessageWithId Message;
		public long CurrentCachePosition;
		public long MaxCachePosition;
	}

	public sealed class ReadResult {
		public int ReadRecords;
		public long CurrentCachePosition;
		public long StartingCachePosition;
		public long AvailableCachePosition;
		public bool ReadEndOfCacheBeforeItWasFlushed;
	}

	public sealed class ReadBulkResult {
		public int ReadRecords;
		public long CurrentCachePosition;
		public long StartingCachePosition;
		public long AvailableCachePosition;
		public bool ReadEndOfCacheBeforeItWasFlushed;
		public IList<MessageHandlerClosure> Messages;

	}


	

	public sealed class FetchResult {
		public long CachedRemotePosition;
		public long ActualRemotePosition;
		public long LocalStoragePosition;
		public long DownloadedBytes;
		public long DownloadedRecords;
		public long UsedBytes;
		public long SavedBytes;
	}

	public sealed class CacheFetcher {
		readonly CloudPageReader _remote;
		readonly CloudCheckpointReader _remotePos;

		public const string CachePositionName = "cache-v2.pos";
		public const string CacheStreamName = "cache-v2.dat";


		readonly FileStream _cacheWriter;
		
		readonly FileCheckpointArrayWriter _cacheChk;

		readonly IMemoryStreamManager _streamManager;
		readonly BinaryWriter _writer;
		readonly static Encoding CacheFormat = new UTF8Encoding(false);
		public readonly string StreamName;
		

		public static CacheFetcher CreateStandalone(string sas, string stream, DirectoryInfo folder) {
			var fetcher = new CacheFetcher(sas, stream, folder, new MemoryStreamFactoryManager());
			fetcher.Init();
			return fetcher;
		}

		public CacheReader CreateReaderInstance() {
			var cacheReader = _outputFile.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
			var checkpointReader = new FileCheckpointArrayReader(_outputCheckpoint, 2);
			return new CacheReader(_cacheChk, cacheReader,checkpointReader);
		}

		public CacheFetcher(string sas, string stream, DirectoryInfo folder, IMemoryStreamManager streamManager) {
			StreamName = stream;
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
			_outputFile = new FileInfo(Path.Combine(di.FullName, CacheStreamName));
			_outputCheckpoint = new FileInfo(Path.Combine(di.FullName, CachePositionName));

			_outputFile.Refresh();
			_cacheWriter = _outputFile.Open(_outputFile.Exists ? FileMode.Open : FileMode.Create, FileAccess.ReadWrite, FileShare.Read);
			
			_writer = new BinaryWriter(_cacheWriter,CacheFormat);
			_cacheChk = new FileCheckpointArrayWriter(_outputCheckpoint, 2);
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

		public int AmountToLoadMax = 10*1024*1024;
		FileInfo _outputFile;
		FileInfo _outputCheckpoint;

		public void Init() {
			_cacheChk.GetOrInitPosition();
		}

		
	

		public async Task<FetchResult> DownloadNext() {
			var pos = _cacheChk.ReadPositionVolatile();

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
			long downloadedRecords = 0;

			using (var mem = _streamManager.GetStream("fetcher")) {
				await _remote.DownloadRangeToStreamAsync(mem, cachedRemotePos, (int) amountToLoad)
					.ConfigureAwait(false);
				// cool, we've got some data back

				result.DownloadedBytes = mem.Position;
				mem.Seek(0, SeekOrigin.Begin);


				_cacheWriter.Seek(convertedLocalPos, SeekOrigin.Begin);

				var pages = new List<MessageWithId>();
				using (var bin = new BinaryReader(mem)) {
					MessageWithId msg;
					while (TryRead(bin, out msg)) {
						var hasMorePagesToRead = HasMore(msg);

						if (false == hasMorePagesToRead && pages.Count == 0) {
							// fast path, we can save message directly without merging
							WriteMessage(msg);
							downloadedRecords += 1;
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
							downloadedRecords +=1;
						}
						pages.Clear();

					}
					
				}
				if (usedBytes == 0) {
					return result;
				}
				_writer.Flush();
				_cacheWriter.Flush(true);
				_cacheChk.Update(new[] {
					_cacheWriter.Position,
					cachedRemotePos + usedBytes,
				});

				result.UsedBytes = usedBytes;
				result.SavedBytes = _cacheWriter.Position - result.LocalStoragePosition;
				result.DownloadedRecords = downloadedRecords;
				return result;
			}
		}

		

		void WriteMessage(MessageWithId msg) {
			EnsureSizeToAvoidFragmentation();
			CacheStorage.Write(_writer, msg);
		}

		void EnsureSizeToAvoidFragmentation()
		{
			const long shouldHaveMb = 2*1024*1024;
			const long increaseByMb = 5*shouldHaveMb;
			var current = _cacheWriter.Length;
			var pos = _cacheWriter.Position;

			var available = current - pos;
			if (available < shouldHaveMb) {
				_cacheWriter.SetLength(_cacheWriter.Length + increaseByMb);
			}
		}

		static bool HasMore(MessageWithId msg) {
			var hasMore = ((MessageFlags) msg.Attributes & MessageFlags.ToBeContinued) ==
			              MessageFlags.ToBeContinued;
			return hasMore;
		}
	}

}