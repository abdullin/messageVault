using System.Collections.Generic;
using System.IO;
using MessageVault.Api;
using NUnit.Framework;

namespace MessageVault.Tests {

	[TestFixture]
	public sealed class MessageFramerTests {
		static MessageToWrite[] GenerateMessages(int count) {
			var result = new List<MessageToWrite>();
			for (int i = 0; i < count; i++) {
				var key = "key" + i;
				var bytes = new byte[i + i % 7 * 21];

				for (int j = 0; j < bytes.Length; j++) {
					bytes[j] = (byte) j;
				}


				result.Add(new MessageToWrite(0, key, bytes));
			}
			return result.ToArray();
		}

		void AssertEqual(MessageToWrite[] a, MessageToWrite[] b) {
			Assert.AreEqual(a.Length, b.Length);

			for (int i = 0; i < a.Length; i++) {
				var ma = a[i];
				var mb = b[i];

				Assert.AreEqual(ma.Key, mb.Key);
				CollectionAssert.AreEqual(ma.Value, mb.Value);
			}
		}


		[Test]
		public void ValidHash() {
			using (var mem = new MemoryStream()) {
				var messages = GenerateMessages(7);
				var hash = ApiMessageFramer.WriteMessages(messages, mem);
				mem.Seek(0, SeekOrigin.Begin);

				var actual = ApiMessageFramer.ReadMessages(mem, hash);

				AssertEqual(messages, actual);
			}
		}

		[Test, ExpectedException(typeof(InvalidDataException))]
		public void InValidHash() {
			using (var mem = new MemoryStream()) {
				var messages = GenerateMessages(7);
				var hash = ApiMessageFramer.WriteMessages(messages, mem);
				hash[0] += 1;
				mem.Seek(0, SeekOrigin.Begin);

				var actual = ApiMessageFramer.ReadMessages(mem, hash);

				AssertEqual(messages, actual);
			}
		}
	}

}