using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MessageVault.MemoryPool;

namespace MessageVault.Cloud {

	public sealed class MessageCopier : IDisposable
	{
		readonly IPageReader _sourceReader;
		readonly ICheckpointReader _sourcePos;
		readonly IMemoryStreamManager _streamManager;
		readonly IPageWriter _targetWriter;
		readonly ICheckpointWriter _targetPos;

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

		public void Init()
		{
			_targetWriter.Init();
			_targetPos.GetOrInitPosition();
		}


		public async Task Run(CancellationToken token)
		{
			while (!token.IsCancellationRequested)
			{
				var result = await CopyNextBatch(token).ConfigureAwait(false);
				if (result == 0)
				{
					await Task
						.Delay(1000, token)
						.ConfigureAwait(false);
				}
			}
		}


		public async Task<long> CopyNextBatch(CancellationToken token)
		{
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

		bool _disposed = false;
		readonly object _disposeLock = new object();

		public void Dispose()
		{

			lock (_disposeLock)
			{
				if (_disposed)
				{
					return;
				}

				using (_targetPos)
				using (_targetWriter)
				using (_sourcePos)
				using (_sourceReader)
				{
					_disposed = true;
				}
			}
		}
	}

}