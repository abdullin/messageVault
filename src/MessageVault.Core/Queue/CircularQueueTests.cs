using System;
using System.IO;
using NUnit.Framework;

namespace MessageVault.Queue {

	[TestFixture]
	public sealed class CircularQueueTests {

		CircularQueue _instance;
		string _prefixPath;


		void Create(int size) {
			var folderName = Path.Combine(Path.GetTempPath(), "test");
			if (Directory.Exists(folderName)) {
				Directory.Delete(folderName, true);
			}
			Directory.CreateDirectory(folderName);

			_prefixPath = Path.Combine(folderName, "base");

			_instance = CircularQueue.Create(_prefixPath, size);
			
		}

		[TearDown]
		public void TearDown() {
			if (_instance != null) {
				_instance.Dispose();
			}
		}


		void FillBytes(MemoryStream stream, int count) {
			for (int i = 0; i < count; i++) {
				stream.Write(new [] {(byte)(i+1)},0,1);
			}
		}

		[Test]
		public void WriteReadOne() {
			Create(10000);

			using (var expected = new MemoryStream()) {
				FillBytes(expected, 10000);
				expected.Seek(0, SeekOrigin.Begin);
				_instance.Append(expected, (int)expected.Length);

				var count = _instance.Consume(i => new MemoryStream(), stream => {
					StreamsAreEqual(expected, stream);
				});
				Assert.AreEqual(10000, count);
			}
		}



		[Test]
		public void ThreeLargeMessages()
		{
			var size = 10000;
			Create(size);

			for (int i = 0; i < 4; i++) {
				using (var expected = new MemoryStream())
				{
					FillBytes(expected, size/3);
					expected.Seek(0, SeekOrigin.Begin);
					_instance.Append(expected, (int)expected.Length);

					var count = _instance.Consume(_ => new MemoryStream(), stream => {
						StreamsAreEqual(expected, stream);
					});
					Assert.AreEqual(expected.Length, count);
				}
			}
		}


		[Test]
		public void WriteManyReadOne()
		{
			var size = 10000;
			Create(size);


			for (int j = 0; j < 11; j++) {
				using (var expected = new MemoryStream())
				{
					for (int i = 1; i < 10000; i++)
					{
						if ((i + expected.Position) > size)
						{
							break;
						}
						using (var step = new MemoryStream())
						{
							FillBytes(step, i);
							_instance.Append(step, i);
							step.Seek(0, SeekOrigin.Begin);
							step.WriteTo(expected);
						}
					}


					var count = _instance.Consume(i => new MemoryStream(), stream => {
						StreamsAreEqual(expected, stream);
					});

					Assert.AreEqual(expected.Length, count);
				}
			}

			
		}

		void StreamsAreEqual(MemoryStream e, MemoryStream a) {
			Assert.AreEqual(e.Length, a.Length, "Length");
			CollectionAssert.AreEqual(e.ToArray(), a.ToArray());
		}
	
		[Test]
		public void SyntheticLoop() {
			int maxSize = 1000;
			Create(maxSize);
			for (int j = 1; j < maxSize; j++)
			{
				using (var expected = new MemoryStream())
				{
					
					FillBytes(expected, j % maxSize);
					expected.Seek(0, SeekOrigin.Begin);
					_instance.Append(expected, (int)expected.Length);

					var count = _instance.Consume(i => new MemoryStream(), stream => {
						StreamsAreEqual(expected, stream);
					});
					Assert.AreEqual(j, count, "Expected count with j=" +j);
				}
			}
		}


		//}
		[Test]
		public void SyntheticLoopWithReloading() {
			int restartCount = 0, byteCount = 0;

			int maxSize = 1000+1;
			Create(maxSize);
			for (int j = 1; j < maxSize; j++)
			{
				using (var expected = new MemoryStream())
				{
					var fillSize = j % maxSize;
					byteCount += fillSize;
					FillBytes(expected, fillSize);
					expected.Seek(0, SeekOrigin.Begin);
					_instance.Append(expected, (int)expected.Length);

					if (j%7 == 1) {
						restartCount += 1;
						_instance.Dispose();
						_instance = CircularQueue.Create(_prefixPath, maxSize);
						
					}

					var count = _instance.Consume(i => new MemoryStream(), stream => {
						StreamsAreEqual(expected, stream);
					});
					Assert.AreEqual(j, count, "Expected count with j=" + j);
				}
			}

			Console.WriteLine("{0} restarts, {1} bytes written", restartCount, byteCount);
		}

	}

}