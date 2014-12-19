using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Core;

namespace MessageVault {

	public sealed class PageReader {
		readonly CloudPageBlob _blob;
		
		public PageReader(CloudPageBlob blob) {
			_blob = blob;
		}

		
		const int Limit = 1024 * 1024 * 4;
		readonly byte[] _buffer = new byte[Limit];

		public MessageResult ReadMessages(long from, long till, int maxCount) {
			Require.ZeroOrGreater("from", from);
			Require.ZeroOrGreater("maxOffset", till);
			Require.Positive("maxCount", maxCount);

			var list = new List<Message>(maxCount);
			var position = from;

			using (var prs = new PageReadStream(Downloader, from, till, _buffer)) {
				using (var bin = new BinaryReader(prs)) {
					while (prs.Position < prs.Length)
					{
						var message = Message.Read(bin);
						list.Add(message);
						position = prs.Position;
						if (list.Count >= maxCount)
						{
							break;
						}
					}		
				}
				
			}
			
			
			return new MessageResult(list, position);

		}

		void Downloader(Stream stream, long pageOffset, long length) {
			_blob.DownloadRangeToStream(stream, pageOffset, length);

		}


	}

}
