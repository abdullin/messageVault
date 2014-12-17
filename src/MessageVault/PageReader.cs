using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MessageVault {

	public sealed class PageReader {
		readonly CloudPageBlob _blob;

		public PageReader(CloudPageBlob blob) {
			_blob = blob;
		}

		public ICollection<StoredMessage> ReadMessages(long from, long maxOffset, int maxCount) {
			Require.ZeroOrGreater("from", from);
			Require.ZeroOrGreater("maxOffset", maxOffset);
			Require.Positive("maxCount", maxCount);
			
			// TODO: include a filter
			var list = new List<StoredMessage>(maxCount);
			using (var stream = _blob.OpenRead())
			{
				stream.Seek(from, SeekOrigin.Begin);

				using (var binary = new BinaryReader(stream, Encoding.UTF8, true))
				{
					while (stream.Position < maxOffset)
					{
						// TODO: use buffers
						var version = binary.ReadByte();
						if (version != Constants.ReservedFormatVersion)
						{
							throw new InvalidOperationException("Unknown storage format");
						}
						var id = binary.ReadBytes(16);
						var contract = binary.ReadString();
						var len = binary.ReadInt32();
						var data = binary.ReadBytes(len);
						var uuid = new MessageId(id);
						list.Add(new StoredMessage(uuid, contract, data));
						if (list.Count >= maxCount) {
							break;
						}
						
					}
				}
			}
			return list;

		} 

		public IEnumerable<StoredMessage> Read(long from, long count) {
			using (var stream = _blob.OpenRead()) {
				stream.Seek(from, SeekOrigin.Begin);

				using (var binary = new BinaryReader(stream, Encoding.UTF8, true)) {
					while (stream.Position < (from + count)) {
						// TODO: use buffers
						var version = binary.ReadByte();
						if (version != Constants.ReservedFormatVersion) {
							throw new InvalidOperationException("Unknown storage format");
						}
						var id = binary.ReadBytes(16);
						var contract = binary.ReadString();
						var len = binary.ReadInt32();
						var data = binary.ReadBytes(len);
						var uuid = new MessageId(id);
						yield return new StoredMessage(uuid, contract, data);
					}
				}
			}
		}

	}

}
