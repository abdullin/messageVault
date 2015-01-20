using MessageVault.Memory;
using NUnit.Framework;


// ReSharper disable InconsistentNaming
namespace MessageVault.Tests {

    [TestFixture]
	public sealed class SyntheticMemoryTests : SyntheticTestBase{

		

		[SetUp]
		public void Setup() {
			var pages = new MemoryPageReaderWriter();
			var checkpoint = new MemoryCheckpointReaderWriter();

			_writer = new MessageWriter(pages, checkpoint);
			_reader = new MessageReader(checkpoint, pages);
		    _checkpointReader = checkpoint;
		    _pageWriter = pages;
			_writer.Init();
		}

	




	}

}