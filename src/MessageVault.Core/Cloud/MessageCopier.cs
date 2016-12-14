using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MessageVault.MemoryPool;

namespace MessageVault.Cloud {

	public sealed class MessageCopier : IDisposable
	{
		readonly IPageReader _sourceReader;
		public readonly ICheckpointReader SourcePos;
		readonly IMemoryStreamManager _streamManager;
		readonly IPageWriter _targetWriter;
		public readonly ICheckpointWriter TargetPos;

		public MessageCopier(
			IPageReader sourceReader,
			ICheckpointReader sourcePos,
			IMemoryStreamManager streamManager,
			IPageWriter targetWriter,
			ICheckpointWriter targetPos)
		{
			_sourceReader = sourceReader;
			SourcePos = sourcePos;
			_streamManager = streamManager;
			_targetWriter = targetWriter;
			TargetPos = targetPos;
		}

		public int AmountToLoadMax = 4 * 1024 * 1024;

		public void Init()
		{
			_targetWriter.Init();
			TargetPos.GetOrInitPosition();
		}


		public async Task Run(CancellationToken token)
		{
			while (!token.IsCancellationRequested)
			{
				var result = await CopyNextBatch(token).ConfigureAwait(false);
				if (result.CopiedBytes == 0)
				{
					await Task
						.Delay(1000, token)
						.ConfigureAwait(false);
				}
			}
		}

		public struct CopyResult {
			public readonly long CopiedBytes;
			public readonly long MaxPos;
			public readonly long CopyStartPos;
			public readonly long CopyEndPos;


			public CopyResult(long copiedBytes, long maxPos, long copyStartPos, long copyEndPos) {
				CopiedBytes = copiedBytes;
				MaxPos = maxPos;
				CopyStartPos = copyStartPos;
				CopyEndPos = copyEndPos;
			}
		}


		public async Task<CopyResult> CopyNextBatch(CancellationToken token)
		{
			var maxPos = await SourcePos
				.ReadAsync(token)
				.ConfigureAwait(false);

			var localPos = TargetPos.ReadPositionVolatile();


			var availableAmount = maxPos - localPos;
			if (availableAmount <= 0)
			{
				// we don't have anything to write
				return new CopyResult(0, maxPos, localPos,localPos);
			}

			var amountToLoad = Math.Min(availableAmount, AmountToLoadMax);

			using (var mem = _streamManager.GetStream("fetcher"))
			{
				await _sourceReader.DownloadRangeToStreamAsync(mem, localPos, (int)amountToLoad)
					.ConfigureAwait(false);

				mem.Seek(0, SeekOrigin.Begin);
				_targetWriter.Save(mem, localPos);
				var position = localPos + mem.Length;
				TargetPos.Update(position);
				
				return new CopyResult(mem.Length, maxPos, localPos, position);
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

				using (TargetPos)
				using (_targetWriter)
				using (SourcePos)
				using (_sourceReader)
				{
					_disposed = true;
				}
			}
		}
	}

}