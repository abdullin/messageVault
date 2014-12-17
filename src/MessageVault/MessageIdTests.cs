using NUnit.Framework;

namespace MessageVault {

	[TestFixture]
	public sealed class MessageIdTests {
		[Test]
		public void Roundtrip() {
			var id = MessageId.CreateNew(10);
			
			var bytes = id.GetBytes();
			var restored = new MessageId(bytes);
			Assert.AreEqual(id.GetOffset(), restored.GetOffset(), "offset");
			Assert.AreEqual(id.GetTimeUtc(), restored.GetTimeUtc(), "time");
			Assert.AreEqual(id.GetRand(), restored.GetRand(), "rand");
		}

		[Test]
		public void Empty() {
			Assert.IsTrue(MessageId.Empty.IsEmpty());
			Assert.IsTrue(default(MessageId).IsEmpty());
			Assert.IsTrue(new MessageId(0, 0, 0, 0).IsEmpty());
		}

		[Test]
		public void Comparable()
		{
			var a = MessageId.CreateNew(0);
			var b = MessageId.CreateNew(0);
			Assert.Greater(b, a);
			Assert.Less(a,b);
		}
		[Test]
		public void Equatable() {
			var a = MessageId.CreateNew(0);
			var b = MessageId.CreateNew(0);
			Assert.AreEqual(a, a);
			Assert.AreNotEqual(a, b);
		}

		[Test]
		public void RandIncrements() {
			var a = MessageId.CreateNew(0);
			var b = MessageId.CreateNew(0);
			Assert.IsTrue(a.GetRand()+1 == b.GetRand());
		}

		[Test]
		public void OffsetRoundtrip() {
			long value = 1;
			for (int i = 0; i < 48; i++) {
				var created = MessageId.CreateNew(value);

				Assert.AreEqual(value, created.GetOffset(), "instance " + created);
				var rehydrated = new MessageId(created.GetBytes());
				Assert.AreEqual(value, rehydrated.GetOffset(), "roundtrip");

				value = value << 1;
			}
		}

		[Test]
		public void TimestampRoundtrip()
		{
			long value = 1;
			for (int i = 0; i < 48; i++)
			{
				var created = new MessageId(i,0,0);

				var actual = MessageId.Epoch.AddMilliseconds(i);
				Assert.AreEqual(actual, created.GetTimeUtc(), "instance " + created);
				var rehydrated = new MessageId(created.GetBytes());
				Assert.AreEqual(actual, rehydrated.GetTimeUtc(), "roundtrip");

				value = value << 1;
			}
		}
		[Test]
		public void RandRoundtrip()
		{
			long value = 1;
			for (int i = 0; i < 32; i++)
			{
				var created = new MessageId(0, 0, i);

				
				Assert.AreEqual(i, created.GetRand(), "instance " + created);
				var rehydrated = new MessageId(created.GetBytes());
				Assert.AreEqual(i, rehydrated.GetRand(), "roundtrip");

				value = value << 1;
			}
		}



	}

}