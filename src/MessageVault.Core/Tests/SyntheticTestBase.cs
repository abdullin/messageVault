using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;

namespace MessageVault.Tests {

    public abstract class SyntheticTestBase {
        protected MessageWriter Writer;
        protected MessageReader Reader;
        protected IPageWriter PageWriter;
        protected ICheckpointReader CheckpointReader;
        [Test]
        public void given_empty_when_check_position()
        {

            Assert.AreEqual(0, Writer.GetPosition());
            Assert.AreEqual(0, Reader.GetPosition());
        }


        [Test, ExpectedException(typeof(ArgumentException))]
        public void append_throws_on_empty_collection()
        {
            Writer.Append(new Message[0]);
        }

        static byte[] RandBytes(long len)
        {
            var randBytes = new byte[len];
            new Random().NextBytes(randBytes);
            return randBytes;
        }

        [Test]
        public void given_empty_when_write_message()
        {
            var write = Message.Create("test", RandBytes(200));
            var result = Writer.Append(new[] { write });

            Assert.AreNotEqual(0, result);
            Assert.AreEqual(result, Writer.GetPosition());
            Assert.AreEqual(result, CheckpointReader.Read());
        }

        [Test]
        public void given_one_written_message_when_read_from_start()
        {
            // given
			var write = Message.Create("test", RandBytes(200));
            var result = Writer.Append(new[] { write });
            // when
            var read = Reader.ReadMessages(0, result, 100);
            // expect
            Assert.AreEqual(result, read.NextOffset);
            CollectionAssert.IsNotEmpty(read.Messages);
            Assert.AreEqual(1, read.Messages.Count);
            var msg = read.Messages.First();
            CollectionAssert.AreEqual(write.Key, msg.Key);
            CollectionAssert.AreEqual(write.Value, msg.Value);
            Assert.AreEqual(0, msg.Id.GetOffset());
        }

        [Test]
        public void given_two_written_messages_when_read_from_offset()
        {


        }


        [Test]
        public void quasi_random_test()
        {
            var maxCommitSize = PageWriter.GetMaxCommitSize();
            var written = new List<Message>();
            for (int i = 0; i < 100; i++)
            {
                var batchSize = (i % 10) + 1;
                var list = new Message[batchSize];
                for (int j = 0; j < batchSize; j++)
                {
                    var size = ((i * 1024 + j + 3) % (maxCommitSize - 512)) %Constants.MaxValueSize;
					var write = Message.Create("{0}:{1}", RandBytes(size + 1));
                    list[j] = write;
                }
                Writer.Append(list);
                written.AddRange(list);
            }
        }
    }

}