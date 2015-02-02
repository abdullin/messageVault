using System.IO;
using MessageVault.Files;
using NUnit.Framework;

namespace MessageVault.Tests {

    public sealed class SyntheticFileTests : SyntheticTestBase {
        string _folder;

        [SetUp]
        public void Setup() {

            _folder = Path.Combine(Path.GetTempPath(), "syntethic_test");
            if (!Directory.Exists(_folder)) {
                Directory.CreateDirectory(_folder);
            }

            var streamFile = new FileInfo(Path.Combine(_folder, Constants.StreamFileName));
            var checkFile = new FileInfo(Path.Combine(_folder, Constants.PositionFileName));

            var pageWriter = new FilePageWriter(streamFile);
            var pageReader = new FilePageReader(streamFile);
            var checkReader = new FileCheckpointReader(checkFile);
            var checkWriter = new FileCheckpointWriter(checkFile);

            
            Writer = new MessageWriter(pageWriter, checkWriter);
            Reader = new MessageReader(checkReader, pageReader);
            CheckpointReader = checkReader;
            PageWriter = pageWriter;
            Writer.Init();
        }
        [TearDown]
        public void TearDown() {
            Writer.Dispose();
            Reader.Dispose();
            Directory.Delete(_folder, true);
        }
    }

}