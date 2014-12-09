using System.Collections.Generic;
using System.Text;
using Microsoft.WindowsAzure.Storage.Blob;
using NUnit.Framework;

namespace MessageVault {

	public sealed class AzureSegmentTests {
		CloudBlobContainer _folder;


		[TestFixtureSetUp]
		public void Setup() {
			_folder = TestEnvironment.GetTestContainer(this);

			Logging.InitTrace();
		}

		SegmentWriter CreateWriter(string name) {
			return SegmentWriter.Create(_folder, name);
		}

		static readonly byte[] SmallMessage = Encoding.UTF8.GetBytes("test-me");


		[TestFixtureTearDown]
		public void Teardown() {
			_folder.Delete();
		}

		[Test]
		public void EmptyInit() {
			var segment = CreateWriter("check1");
			Assert.AreEqual(0, segment.Position, "position");

		}

		[Test]
		public void SingleWrite() {
			var segment = CreateWriter("single-write");
			var word = Encoding.UTF8.GetBytes("test-me");
			segment.Append(new[] {word});

			Assert.AreEqual(word.Length + 4, segment.Position, "position");
		}

		[Test]
		public void BatchWrite() {
			var segment = CreateWriter("short-batch-write");

			segment.Append(new[] {SmallMessage, SmallMessage});

			var expected = 2 * (SmallMessage.Length + 4);
			Assert.AreEqual(expected, segment.Position, "position");
		}

		[Test]
		public void SequentialWrite() {
			var segment = CreateWriter("check1");

			segment.Append(new[] {SmallMessage});
			segment.Append(new[] {SmallMessage});

			var expected = 2 * (SmallMessage.Length + 4);
			Assert.AreEqual(expected, segment.Position);
		}

		[Test]
		public void WriteAndReopen() {

			var writer = CreateWriter("reopen");

			writer.Append(new[] { SmallMessage });
			writer.Append(new[] { SmallMessage });

			var writer2 = CreateWriter("reopen");

			var expected = 2 * (SmallMessage.Length + 4);
			Assert.AreEqual(expected, writer2.Position);

		}
		[Test]
		public void WriteLargeBatch() {
			var writer = CreateWriter("large");

			long accumulated = 0;
			var batch = new List<byte[]>();
			while (accumulated < SegmentWriter.BufferSize) {
				batch.Add(SmallMessage);
				accumulated += SmallMessage.Length;
			}
			writer.Append(batch);


		}
	}

}