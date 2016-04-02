using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MessageVault.Api;
using MessageVault.Cloud;
using MessageVault.Files;
using Serilog;

namespace MessageVault {




	public sealed class StreamSas {
		public string Name;
		public string Sas;
	}

	public sealed class MassiveDownloader {
		readonly StreamSas[] _sas;
		readonly IMemoryStreamManager _manager;

		readonly string _cacheRoot;
		
		public MassiveDownloader(StreamSas[] sas, IMemoryStreamManager manager, string cacheRoot) {
			_sas = sas;
			_manager = manager;
			_cacheRoot = cacheRoot;
		}

		


		MessageDownloader Downloader(string sas, string name, string root) {
			var path = Path.Combine(root, "mv-cache", name);
			var dir = new DirectoryInfo(path);

			var writer = FileSetup.CreateAndInitRaw(dir, name);

			var reader = CloudSetup.GetReaderRaw(sas);

			var downloader = new MessageDownloader(reader.Item1, reader.Item2, writer.Item1, writer.Item2,_manager);
			downloader.Init();
			return downloader;

		}


		public void Run(CancellationToken token) {

			var exceptions = new List<Exception>();

			var items = _sas.Select(s => Downloader(s.Sas,s.Name, _cacheRoot)).ToList();

			while (!token.IsCancellationRequested)
			{
				exceptions.Clear();
				var tasks = items.Select(d => d.SyncNext()).ToArray();
				Task.WaitAll(tasks, token);

				int downloaded = 0;


				foreach (var task in tasks)
				{
					if (task.Exception != null)
					{
						exceptions.Add(task.Exception);
					}
					else
					{
						downloaded += task.Result;
					}
				}
				if (exceptions.Any())
				{
					throw new AggregateException("Failured while downloading from MV", exceptions);
				}

				if (downloaded == 0)
				{
					token.WaitHandle.WaitOne(TimeSpan.FromSeconds(1));
				}
			}
		}
	}

	public sealed class MessageDownloader {
		public readonly string StreamName;
		readonly CloudCheckpointReader _position;
		readonly CloudPageReader _page;

		readonly FileCheckpointWriter _outputPosition;
		readonly FilePageWriter _writer;
		readonly IMemoryStreamManager _manager;


		public MessageDownloader(
			CloudCheckpointReader position,
			CloudPageReader page, 
			FileCheckpointWriter outputPosition, 
			FilePageWriter writer,
			IMemoryStreamManager manager) {
			_position = position;
			_page = page;
			_outputPosition = outputPosition;
			_writer = writer;
			_manager = manager;
		}


		public async Task<int> SyncNext() {
			var localPos = _outputPosition.GetOrInitPosition();
			var remotePos = _position.Read();

			var amountToDownload = HowMuchToDownload(localPos, remotePos);
			if (amountToDownload == 0) {
				return 0;
			}

			using (var mem = _manager.GetStream("download", amountToDownload)) {
				await _page.DownloadRangeToStreamAsync(mem, localPos, amountToDownload);
				_writer.EnsureSize(localPos + amountToDownload);
				_writer.Save(mem, localPos);
				return amountToDownload;
			}
		}

		int HowMuchToDownload(long localPos, long remotePos) {
			var amount = remotePos - localPos;
			if (amount < 0)throw new NotSupportedException("source stream changed");
			return (int)Math.Min(amount, 10*1024*1024);
		}

		public void Init() {
			_writer.Init();
			_outputPosition.GetOrInitPosition();
		}
	}

}