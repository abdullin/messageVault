using System;
using System.IO;
using NUnit.Framework;
// ReSharper disable InconsistentNaming
namespace MessageVault.Tests {

	[TestFixture]
	public sealed class IsolatedMessageWriterTests {

		MessageWriter _writer;
		MemoryPages _pages;
		MemoryCheckpoint _checkpoint;

		[SetUp]
		public void Setup() {
			_pages = new MemoryPages();
			_checkpoint = new MemoryCheckpoint();
			_writer = new MessageWriter(_pages, _checkpoint, "test");
			_writer.Init();
		}


		[Test]
		public void empty_writer_has_zero_position() {
			Assert.AreEqual(0, _writer.GetPosition());
		}

		[Test, ExpectedException(typeof(ArgumentException))]
		public void append_throws_on_empty_collection() {
			_writer.Append(new IncomingMessage[0]);
		}


		[Test]
		public void appending_single_message_advances_position() {
			var result = _writer.Append(new[] {new IncomingMessage("test", new byte[10])});
			Assert.AreNotEqual(0, result);
			Assert.AreEqual(result, _writer.GetPosition());
			Assert.AreEqual(result, _checkpoint.Position);
			Assert.AreNotEqual(0, _pages.GetLength());
		}

		sealed class MemoryCheckpoint : ICheckpointWriter {
			public long Position;
			public long GetOrInitPosition() {
				return Position;
			}

			public void Update(long position) {
				Position = position;
			}
		}

		sealed class MemoryPages : IPageWriter {
			MemoryStream _stream;
			public void Init() {
				_stream = new MemoryStream();
			}

			public long GetLength() {
				return _stream.Length;
			}

			long NextSize() {
				return _stream.Length + GetMaxCommitSize();
			}

			public void EnsureSize(long size) {
				Require.OffsetMultiple("size", size, GetPageSize());
				var target = _stream.Length;
				while (size > target) {
					target = NextSize();
				}
				_stream.SetLength(target);
				
			}

			public byte[] ReadPage(long offset) {
				Require.OffsetMultiple("offset", offset, GetPageSize());

				var buffer = new byte[GetPageSize()];
				_stream.Seek(offset,SeekOrigin.Begin);
				_stream.Read(buffer, 0, buffer.Length);
				return buffer;
			}

			public void Save(Stream stream, long offset) {
				_stream.Seek(offset, SeekOrigin.Begin);
				stream.CopyTo(_stream);
			}

			public int GetMaxCommitSize() {
				return 4 * 1024;
			}

			public int GetPageSize() {
				return 512;
			}
		}

	}


}