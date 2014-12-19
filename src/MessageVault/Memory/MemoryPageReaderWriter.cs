using System.IO;

namespace MessageVault.Memory {

	public sealed class MemoryPageReaderWriter : IPageWriter, IPageReader {
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

		public void DownloadRangeToStream(Stream stream, long offset, int length) {
			var buf = _stream.GetBuffer();
			stream.Write(buf, (int)offset, length);
		}
	}

}