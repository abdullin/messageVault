using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
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


			var command = args[0];
			var cachePath = args[1];
			// requires exclusive access to writer

			switch (command.ToLowerInvariant()) {
				case "repair":
					Repair(cachePath);
					return;
				case "backup":
					Backup(cachePath, args[2]);
					return;
			}
			
			
			//
		}

		public static void CopyStream(Stream input, Stream output, long bytes)
		{
			byte[] buffer = new byte[32768];
			int read;
			var counter = 0;
			
			while (bytes > 0 && (read = input.Read(buffer, 0, (int) Math.Min(buffer.Length, bytes))) > 0)
			{
				output.Write(buffer, 0, read);
				bytes -= read;
				counter += 1;

				if ((counter%16384 == 0)) {
					Console.WriteLine("{0:F1} GB to go", 1F*bytes/1024/1024/1024);
				}
			}
		}

		static void Backup(string cachePath, string target) {
			var streamFile = new FileInfo(Path.Combine(cachePath, CacheFetcher.CacheStreamName));
			var checkFile = new FileInfo(Path.Combine(cachePath, CacheFetcher.CachePositionName));

			var posReader = new FileCheckpointArrayReader(checkFile, 2);
			var pos = posReader.Read();
			Console.WriteLine("Pos {0}/{1}", pos[0], pos[1]);

			var dataBackup = Path.Combine(target, CacheFetcher.CacheStreamName + ".gzip");
			var checkBackup = Path.Combine(target, CacheFetcher.CachePositionName + ".bak");
			Console.WriteLine(new DateTime(2016, 4, 6, 12, 0, 0).ToUniversalTime().ToString("O"));
			Console.WriteLine("Will backup to {0}/{1}", dataBackup, checkBackup);
			Console.WriteLine("Press enter to continue");
			Console.ReadLine();


			using (var source = streamFile.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite)) {
				
				using (var output = File.Create(dataBackup)) {
					using (var gzip = new GZipStream(output, CompressionMode.Compress)) {
						CopyStream(source, gzip, pos[0]);
					}
				}
			}
			var checkWrite = new FileCheckpointArrayWriter(new FileInfo(checkBackup), 2);
			checkWrite.GetOrInitPosition();
			checkWrite.Update(pos);

			Console.WriteLine("Backed up to {0}", target);

		}

		static void Repair(string cachePath) {
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
					replaceCheck.Update(new long[] {prev, offset});
					Console.WriteLine("Backup written");
				}


				break;
			}


			Console.ReadLine();
		}
	}
}
