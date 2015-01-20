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

            
            _writer = new MessageWriter(pageWriter, checkWriter);
            _reader = new MessageReader(checkReader, pageReader);
            _checkpointReader = checkReader;
            _pageWriter = pageWriter;
            _writer.Init();
        }
        [TearDown]
        public void TearDown() {
            _writer.Dispose();
            _reader.Dispose();
            Directory.Delete(_folder, true);
        }
    }

}