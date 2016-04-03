using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MessageVault;
using MessageVault.Api;
using MessageVault.Cloud;
using MessageVault.Files;

namespace Cache
{
	class Program
	{

		static void Quit(string message) {
			Console.WriteLine(message);
			Environment.Exit(1);
		}

		static void Main(string[] args)
		{
			if (args.Length < 2) {
				Quit("we need 2 args: connection string and stream name");
			}


			var connection = args[0].Trim('"','\'').Split(new[] {';'}, StringSplitOptions.RemoveEmptyEntries);
			var streamName = args[1];
			var cache = Path.Combine(Directory.GetCurrentDirectory(), "cache");
			if (args.Length > 2) {
				cache =  args[2];
			}

			if (!Directory.Exists(cache)) {
				Directory.CreateDirectory(cache);
			}
			
			var server = connection[0];
			var user = connection[1];
			var pwd = connection[2];

			
			
			
			var client = new CloudClient(server, user, pwd);
			var sas = client.GetReaderSignatureAsync(streamName);
			sas.Wait();

			


			var fetcher = CacheFetcher.CreateStandalone(sas.Result, streamName, new DirectoryInfo(cache));

			var cacheReader = fetcher.CreateReaderInstance();

			using (var source = new CancellationTokenSource()) {
				var token = source.Token;
				var recordCount = 500;
				Task.Factory.StartNew(() => {
					var pos = 0L;
					while (!token.IsCancellationRequested) {
						var result = cacheReader.ReadAll(pos, recordCount, c => { });
						if (result.ReadRecords == 0) {
							token.WaitHandle.WaitOne(100);
							continue;
						}
						Console.WriteLine("Read {0} records",result.ReadRecords);

						pos = result.CurrentCachePosition;
					}
				});


				while (true) {
					var started = Stopwatch.StartNew();
					var downloadTask = fetcher.DownloadNext();
					downloadTask.Wait(token);
					var result = downloadTask.Result;

					var percent = (100*(result.UsedBytes + result.CachedRemotePosition))/
					              result.ActualRemotePosition;
					var usedPerSec = result.UsedBytes/started.Elapsed.TotalSeconds;
					

					Console.WriteLine("Downloaded {0}% at speed {1:F1}. {2} records", percent, usedPerSec, result.DownloadedRecords);

					if (result.DownloadedBytes == 0)
					{
						break;
					}
				}

				source.Cancel();
				Console.WriteLine("Done");
				Console.ReadLine();
			}


			//var async = reader.GetMessageReaderAsync(streamName);
			//async.Wait();

			//var result = async.Result;
			//var remote = result.GetPosition();
			//using (var writer = FileSetup.CreateAndInitWriter(new DirectoryInfo(cache), streamName)) {

			//	var cached = writer.GetPosition();
			//	Console.WriteLine("Remote {0}, local {1}, to cache {2}", remote, cached, remote - cached);

			//	long lastCached = cached;

			//	while (((remote - lastCached)) > 0) {
			//		var downloaded = async.Result.ReadMessages(lastCached, remote, 1000);
			//		var appendResult = writer.Append(
			//			downloaded.Messages.Select(mi => new Message(mi.Attributes, mi.Key, mi.Value, mi.Crc32))
			//				.ToArray());

			//		Console.WriteLine("Next {0}/{1}", downloaded.NextOffset, appendResult.Position);
			//		lastCached = downloaded.NextOffset;
			//	}
			//}

			



		}
	}
}
