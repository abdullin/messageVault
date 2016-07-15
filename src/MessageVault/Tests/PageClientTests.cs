using System;
using System.Threading;
using MessageVault.Api;
using NUnit.Framework;

namespace MessageVault.Tests {

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
				pager.Publish(new[] {unpackedMessage}, s.Token);
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