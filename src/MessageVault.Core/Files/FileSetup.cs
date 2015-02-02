using System.IO;

namespace MessageVault.Files {

    public static class FileSetup {
        public static MessageWriter CreateAndInitWriter(DirectoryInfo folder, string stream) {
            
            var streamDir = Path.Combine(folder.FullName, stream);
            var di = new DirectoryInfo(streamDir);
            if (!di.Exists) {
                di.Create();
            }
            var pagesFile = new FileInfo(Path.Combine(di.FullName, Constants.StreamFileName));
            var checkpointFile = new FileInfo(Path.Combine(di.FullName, Constants.PositionFileName));
            
            var pages = new FilePageWriter(pagesFile);
            var checkpoint = new FileCheckpointWriter(checkpointFile);


            var writer = new MessageWriter(pages, checkpoint);
            writer.Init();
            return writer;
        }

        public static MessageReader GetReader(DirectoryInfo folder, string stream) {
            var streamDir = Path.Combine(folder.FullName, stream);
            var di = new DirectoryInfo(streamDir);
            if (!di.Exists)
            {
                di.Create();
            }
            var pagesFile = new FileInfo(Path.Combine(di.FullName, Constants.StreamFileName));
            var checkpointFile = new FileInfo(Path.Combine(di.FullName, Constants.PositionFileName));

            var pages = new FilePageReader(pagesFile);
            var checkpoint = new FileCheckpointReader(checkpointFile);

            var reader = new MessageReader(checkpoint, pages);
            return reader;
        }
    }

}