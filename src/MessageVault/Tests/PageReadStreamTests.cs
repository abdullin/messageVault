using System.IO;
using System.Text;
using NUnit.Framework;

namespace MessageVault.Tests {

	[TestFixture]
	public sealed class PageReadStreamTests {
		// ReSharper disable InconsistentNaming


		readonly MemoryStream _mem;
		readonly BinaryWriter _given;

		public PageReadStreamTests() {
			_mem = new MemoryStream();
			_given = new BinaryWriter(_mem, Encoding.UTF8, true);
		}

		[SetUp]
		public void Setup() {
			_mem.SetLength(0);
			_mem.Seek(0, SeekOrigin.Begin);
		}

		[TestFixtureTearDown]
		public void FixtureTeardown() {
			_mem.Dispose();
		}

		[Test]
		public void given_small_cache() {
			for (long i = 0; i < 20; i++) {
				_given.Write(i);
			}
			using (var reader = new PageReadStream(Downloader, 0, _mem.Position, new byte[11])) {
				using (var bin = new BinaryReader(reader)) {
					for (long i = 0; i < 20; i++) {
						Assert.AreEqual(i * 8, reader.Position);
						Assert.AreEqual(i, bin.ReadInt64());
					}
				}
			}
		}

		[Test]
		public void given_large_cache() {
			//_mem.Write();
			_given.Write(10L); //8b
			_given.Write(20L);
			using (var reader = new PageReadStream(Downloader, 0, _mem.Position, new byte[1000])) {
				Assert.AreEqual(16, reader.Length);
				using (var bin = new BinaryReader(reader)) {
					Assert.AreEqual(10, bin.ReadInt64());
					Assert.AreEqual(8, reader.Position);
					Assert.AreEqual(20, bin.ReadInt64());
					Assert.AreEqual(16, reader.Position);
				}
			}
		}

		[Test]
		public void given_offset() {
			_given.Write(10L);
			_given.Write(20L);
			using (var reader = new PageReadStream(Downloader, 8, 16, new byte[1000]))
			{
				Assert.AreEqual(16, reader.Length);
				Assert.AreEqual(8, reader.Position);
				using (var bin = new BinaryReader(reader))
				{
					Assert.AreEqual(20, bin.ReadInt64());
					Assert.AreEqual(16, reader.Position);
				}
			}
		}

		void Downloader(Stream stream, long pageOffset, long length) {
			_mem.Seek(pageOffset, SeekOrigin.Begin);
			var buf = _mem.GetBuffer();
			stream.Write(buf, (int) pageOffset, (int) length);
			
		}
	}

}