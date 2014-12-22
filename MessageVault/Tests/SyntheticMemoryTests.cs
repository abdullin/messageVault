using System;
using System.Collections.Generic;
using MessageVault.Memory;
using NUnit.Framework;
using System.Linq;
using Serilog;

// ReSharper disable InconsistentNaming
namespace MessageVault.Tests {

	[TestFixture]
	public sealed class SyntheticMemoryTests {

		MessageWriter _writer;
		MemoryPageReaderWriter _pages;
		MemoryCheckpointReaderWriter _checkpoint;
		MessageReader _reader;

		public SyntheticMemoryTests() {
			Logging.InitTrace();
		}

		[SetUp]
		public void Setup() {
			_pages = new MemoryPageReaderWriter();
			_checkpoint = new MemoryCheckpointReaderWriter();

			_writer = new MessageWriter(_pages, _checkpoint, "test");
			_reader = new MessageReader(_checkpoint, _pages);
			_writer.Init();
		}

		[Test]
		public void given_empty_when_check_position() {
			Log.Information("Test");
			Assert.AreEqual(0, _writer.GetPosition());
			Assert.AreEqual(0, _reader.GetPosition());
		}


		[Test, ExpectedException(typeof(ArgumentException))]
		public void append_throws_on_empty_collection() {
			_writer.Append(new MessageToWrite[0]);
		}

		static byte[] RandBytes(int len) {
			var randBytes = new byte[len];
			new Random().NextBytes(randBytes);
			return randBytes;
		}

		[Test]
		public void given_empty_when_write_message() {
			var write = new MessageToWrite("test", RandBytes(200));
			var result = _writer.Append(new[] { write });

			Assert.AreNotEqual(0, result);
			Assert.AreEqual(result, _writer.GetPosition());
			Assert.AreEqual(result, _checkpoint.Read());
			Assert.AreNotEqual(0, _pages.GetLength());

		}

		[Test]
		public void given_one_written_message_when_read_from_start() {
			// given
			var write = new MessageToWrite("test", RandBytes(200));
			var result = _writer.Append(new[] {write});
			// when
			var read = _reader.ReadMessages(0, result, 100);
			// expect
			Assert.AreEqual(result, read.NextOffset);
			CollectionAssert.IsNotEmpty(read.Messages);
			Assert.AreEqual(1, read.Messages.Count);
			var msg = read.Messages.First();
			Assert.AreEqual(write.Key, msg.Key);
			CollectionAssert.AreEqual(write.Value, msg.Value);
			Assert.AreEqual(0, msg.Id.GetOffset());
		}

		[Test]
		public void given_two_written_messages_when_read_from_offset() {

			
		}


		[Test]
		public void quasi_random_test() {
			var maxCommitSize = _pages.GetMaxCommitSize();
			var written = new List<MessageToWrite>();
			for (int i = 0; i < 100; i++) {
				var batchSize = (i % 10) + 1;
				var list = new MessageToWrite[batchSize];
				for (int j = 0; j < batchSize; j++) {
					var size = (i * 1024 + j + 3) % (maxCommitSize - 512);
					var write = new MessageToWrite("{0}:{1}", RandBytes(size + 1));
					list[j] = write;
				}
				_writer.Append(list);
				written.AddRange(list);
			}
		}




	}

}