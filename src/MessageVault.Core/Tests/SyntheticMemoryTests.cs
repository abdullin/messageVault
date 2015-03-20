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

			Writer = new MessageWriter(pages, checkpoint);
			Reader = new MessageReader(checkpoint, pages);
		    CheckpointReader = checkpoint;
		    PageWriter = pages;
			Writer.Init();
		}
	}

}