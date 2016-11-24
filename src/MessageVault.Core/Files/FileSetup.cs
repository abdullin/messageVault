using System;
using System.IO;
using MessageVault.Cloud;
using MessageVault.MemoryPool;

namespace MessageVault.Files {

	public static class FileSetup {
		public static MessageWriter CreateAndInitWriter(DirectoryInfo folder, string stream) {

			var raw = CreateAndInitRaw(folder, stream);
			var writer = new MessageWriter(raw.Item2, raw.Item1);
			writer.Init();
			return writer;
		}

		public static Tuple<FileCheckpointWriter,FilePageWriter> CreateAndInitRaw(DirectoryInfo folder, string stream) {
			var streamDir = Path.Combine(folder.FullName, stream);
			var di = new DirectoryInfo(streamDir);
			if (!di.Exists)
			{
				di.Create();
			}
			var pagesFile = new FileInfo(Path.Combine(di.FullName, Constants.StreamFileName));
			var checkpointFile = new FileInfo(Path.Combine(di.FullName, Constants.PositionFileName));

			var pages = new FilePageWriter(pagesFile);
			var checkpoint = new FileCheckpointWriter(checkpointFile);
			return Tuple.Create(checkpoint, pages);
		}

		public static MessageReader GetReader(DirectoryInfo folder, string stream) {
			var tuple = GetReaderRaw(folder, stream);

			return new MessageReader(tuple.Item1, tuple.Item2);
		}

		public static Tuple<FileCheckpointReader, FilePageReader> GetReaderRaw(DirectoryInfo folder, string stream) {
			var streamDir = Path.Combine(folder.FullName, stream);
			var di = new DirectoryInfo(streamDir);
			if (!di.Exists) {
				di.Create();
			}
			var pagesFile = new FileInfo(Path.Combine(di.FullName, Constants.StreamFileName));
			var checkpointFile = new FileInfo(Path.Combine(di.FullName, Constants.PositionFileName));

			var pages = new FilePageReader(pagesFile);
			var checkpoint = new FileCheckpointReader(checkpointFile);

			return Tuple.Create(checkpoint, pages);

			
		}


		public static MessageFetcher MessageFetcher(DirectoryInfo folder, string stream, IMemoryStreamManager streamManager = null)
		{
			var manager = streamManager ?? new MemoryStreamFactoryManager();
			var raw = GetReaderRaw(folder, stream);
			var remote = raw.Item2;
			var remotePos = raw.Item1;

			return new MessageFetcher(remote, remotePos, manager, stream);
		}
	}

}