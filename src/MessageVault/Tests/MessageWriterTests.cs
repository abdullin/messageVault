using System;
using System.Collections.Generic;
using MessageVault.Cloud;
using NUnit.Framework;

namespace MessageVault.Tests {

	public sealed class MessageWriterTests {
		string _folder;


		[SetUp]
		public void Setup() {

			_folder = TestEnvironment.GetContainerName(this.GetType().Name);
		}

		MessageWriter CreateWriter(string name) {
			var container = TestEnvironment.Client.GetContainerReference(_folder);
			return CloudSetup.CreateAndInitWriter(container);
		}

		static readonly Message SmallMessage = Message.Create("test", Guid.NewGuid().ToByteArray());


		[TearDown]
		public void Teardown() {
			if (_folder != null) {
				TestEnvironment.Client.GetContainerReference(_folder).DeleteIfExists();
			}
		}

		[Test]
		public void EmptyInit() {
			var segment = CreateWriter("check1");
			Assert.AreEqual(0, segment.GetPosition(), "position");

		}

		[Test]
		public void SingleWrite() {
			var segment = CreateWriter("single-write");
			Assert.AreEqual(0, segment.GetPosition());
			var position = segment.Append(new[] {SmallMessage});

			Assert.AreEqual(position, segment.GetPosition());
		}

		[Test]
		public void BatchWrite() {
			var segment = CreateWriter("short-batch-write");

			var position = segment.Append(new[] {SmallMessage, SmallMessage});


			Assert.AreEqual(position, segment.GetPosition(), "position");
		}

		[Test]
		public void SequentialWrite() {
			var segment = CreateWriter("check1");

			segment.Append(new[] {SmallMessage});
			var position = segment.Append(new[] {SmallMessage});


			Assert.AreEqual(position, segment.GetPosition());
		}

		[Test]
		public void WriteAndReopen() {

			var writer = CreateWriter("reopen");

			writer.Append(new[] { SmallMessage });
			var position = writer.Append(new[] { SmallMessage });

			var writer2 = CreateWriter("reopen");


			Assert.AreEqual(position, writer2.GetPosition());

		}
		[Test]
		public void WriteLargeBatch() {
			var writer = CreateWriter("large");

			long accumulated = 0;
			var batch = new List<Message>();
			var size = writer.GetBufferSize();
			while (accumulated < size) {
				batch.Add(SmallMessage);
				accumulated += SmallMessage.Value.Length;
			}
			writer.Append(batch);


		}
	}

}