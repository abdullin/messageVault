using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MessageVault.MemoryPool;

namespace MessageVault.Cloud {

	public sealed class MessageCopier
	{
		readonly IPageReader _sourceReader;
		readonly ICheckpointReader _sourcePos;
		readonly IMemoryStreamManager _streamManager;

		public readonly IPageWriter _targetWriter;
		public readonly ICheckpointWriter _targetPos;
		


		public MessageCopier(
			IPageReader sourceReader, 
			ICheckpointReader sourcePos, 
			IMemoryStreamManager streamManager, 
			IPageWriter targetWriter, 
			ICheckpointWriter targetPos)
		{
			_sourceReader = sourceReader;
			_sourcePos = sourcePos;
			_streamManager = streamManager;
			_targetWriter = targetWriter;
			_targetPos = targetPos;
		}

		public int AmountToLoadMax = 4 * 1024 * 1024;


	

		public async Task<long> CopyNextBatch(CancellationToken token)
		{
			//var convertedLocalPos = pos[0];
			//var currentRemotePosition = pos[1];


			var maxPos = await _sourcePos
				.ReadAsync(token)
				.ConfigureAwait(false);

			var localPos = _targetPos.GetOrInitPosition();



			

			var availableAmount = maxPos - localPos;
			if (availableAmount <= 0)
			{
				// we don't have anything to write
				return 0;
			}

			var amountToLoad = Math.Min(availableAmount, AmountToLoadMax);

			using (var mem = _streamManager.GetStream("fetcher"))
			{
				await _sourceReader.DownloadRangeToStreamAsync(mem, localPos, (int)amountToLoad)
					.ConfigureAwait(false);

				mem.Seek(0, SeekOrigin.Begin);
				_targetWriter.Save(mem, localPos);
				var position = localPos + mem.Length;
				_targetPos.Update(position);
				return mem.Length;
			}
		}
	}

}