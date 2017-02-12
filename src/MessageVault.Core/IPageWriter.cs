using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace MessageVault {

	/// <summary>
	/// Abstracts page interactions. This way we can put all the complex code in <see cref="MessageWriter"/>
	/// and swap <see cref="IPageWriter"/> implementations to write to file disk or memory (for unit tests).
	/// </summary>
	public interface IPageWriter : IDisposable{
		void Init();
		
		void EnsureSize(long size);
		byte[] ReadPage(long offset);
		void Save(Stream stream, long offset);

		Task SaveAsync(Stream stream, long offset, CancellationToken token);

		int GetMaxCommitSize();
		int GetPageSize();
	}
}