using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MessageVault;
using MessageVault.Api;
using MessageVault.Files;

namespace CacheRepair
{
	class Program
	{
		static void Main(string[] args) {

			var cachePath = args[0];
			// requires exclusive access to writer
			var streamFile = new FileInfo(Path.Combine(cachePath, CacheFetcher.CacheStreamName));
			var checkFile = new FileInfo(Path.Combine(cachePath, CacheFetcher.CachePositionName));
			var replace = new FileInfo(Path.Combine(cachePath, CacheFetcher.CachePositionName + ".fix"));
			var posReader = new FileCheckpointArrayReader(checkFile, 2);
			var vector = posReader.Read();
			var sourceStream = streamFile.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
			var reader = new CacheReader(new FixedCheckpointArrayReader(vector), sourceStream, posReader);
			


			var prev = 0L;
			var pos = 0L;
			var i = 0L;
			while (true) {

				try {
					var result = reader.ReadAll(pos, 1000, (id, position, maxPosition) => { });
					if (result.ReadRecords > 0) {
						prev = pos;
						pos = result.CurrentCachePosition;
						i += 1;
						if ((i%100) == 0) {
							Console.WriteLine("{0}:{1:F1}GB", i, 1F*pos/1024/1024/1024);
						}
						continue;

					}
					Console.WriteLine("We are good");
					return;
				}
				catch (InvalidStorageFormatException ex) {
					Console.WriteLine("Last known position {0}", pos);
					Console.WriteLine("Previous position {0} (-{1} bytes)", prev, pos - prev);
					var previousBlock = reader.ReadAll(prev, 1);
					var msg = previousBlock.Messages.Single();
					var offset = msg.Message.Id.GetOffset();
					Console.WriteLine("Previous offset {0}", offset);


					var replaceCheck = new FileCheckpointArrayWriter(replace, 2);
					replaceCheck.GetOrInitPosition();
					replaceCheck.Update(new long[]{prev, offset});
					Console.WriteLine("Backup written");
					

				}

					
				break;
			}


			Console.ReadLine();

			//
		}
	}
}
