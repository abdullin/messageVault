using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using MessageVault.Api;
using MessageVault.Memory;
using NUnit.Framework;

namespace MessageVault.Tests {

	[TestFixture]
	public sealed class TransferFormatTests {
		static Message[] GenerateMessages(int count) {
			var result = new List<Message>();
			for (int i = 0; i < count; i++) {
				var key = "key" + i;
				var bytes = new byte[i + i % 7 * 21];

				for (int j = 0; j < bytes.Length; j++) {
					bytes[j] = (byte) j;
				}


				result.Add(Message.Create(key, bytes));
			}
			return result.ToArray();
		}

		void AssertEqual(Message[] a, Message[] b) {
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
				TransferFormat.WriteMessages(messages, mem);
				mem.Seek(0, SeekOrigin.Begin);

				var actual = TransferFormat.ReadMessages(mem);

				AssertEqual(messages, actual);
			}
		}

		[Test, ExpectedException(typeof(InvalidDataException))]
		public void InValidHash() {
			using (var mem = new MemoryStream()) {
				var messages = GenerateMessages(7);
				TransferFormat.WriteMessages(messages, mem);

				// zero the hash
				mem.Seek(-16,SeekOrigin.Current);
				mem.Write(new byte[16], 0,4);

				mem.Seek(0, SeekOrigin.Begin);

				var actual = TransferFormat.ReadMessages(mem);

				AssertEqual(messages, actual);
			}
		}
	}

	[TestFixture]
	public sealed class PageClientTests {




		static byte[] GetBytes(int count) {

			var b = new byte[count];

			new Random().NextBytes(b);
			return b;


		}

		[Test]
		public void Roundtrip() {
			Console.WriteLine("Starting...");
			var client = new MemoryClient();
			var passed = false;
			var pager = new PagedClient(client, "test");

			using (var s = new CancellationTokenSource(TimeSpan.FromSeconds(10))) {

				var unpackedMessage = new UnpackedMessage(GetBytes(Constants.MaxKeySize),
					GetBytes(Constants.MaxValueSize * 5 + 1));
				Console.WriteLine("Pub...");
				pager.Publish(new[] {

					unpackedMessage



				}, s.Token);
				Console.WriteLine("Waiting...");

				pager.ChaseEventsForever(s.Token, (id, subscription) => {
					CollectionAssert.AreEqual(unpackedMessage.Value, id.Value);
					CollectionAssert.AreEqual(unpackedMessage.Key, id.Key);
					passed = true;
					Console.WriteLine("Got it" + id);
					s.Cancel();

				});
			}

			if (!passed) {
				Assert.Fail("Failed");
			}



		}
	}

}