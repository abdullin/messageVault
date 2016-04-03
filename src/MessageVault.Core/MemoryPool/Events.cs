// ---------------------------------------------------------------------
// Copyright (c) 2015 Microsoft
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
// ---------------------------------------------------------------------

using System;
using System.Diagnostics.Tracing;

namespace MessageVault.MemoryPool
{

	public sealed partial class RecyclableMemoryStreamManager
	{
		public sealed class Events 
		{
			public static readonly Events Write = new Events();

			public enum MemoryStreamBufferType
			{
				Small,
				Large
			}

			public enum MemoryStreamDiscardReason
			{
				TooLarge,
				EnoughFree
			}
			
			public void MemoryStreamCreated(Guid guid, string tag, int requestedSize) {
				//if (!IsVerboseEnabled) {
				//	return;
				//}
				//Metrics.Counter("app.memorystream.created", requestedSize);
				
			}

			[Event(2, Level = EventLevel.Verbose)]
			public void MemoryStreamDisposed(Guid guid, string tag)
			{
				//if (!IsVerboseEnabled)
				//{
				//	return;
				//}
				//Metrics.Counter("app.memorystream.disposed");
			}

			[Event(3, Level = EventLevel.Critical)]
			public void MemoryStreamDoubleDispose(Guid guid, string tag, string allocationStack, string disposeStack1,
												  string disposeStack2)
			{
				//Metrics.Counter("app.memorystream.doubledispose");
			}

			[Event(4, Level = EventLevel.Error)]
			public void MemoryStreamFinalized(Guid guid, string tag, string allocationStack)
			{
				//Metrics.Counter("app.memorystream.finalized");
			}

			[Event(5, Level = EventLevel.Verbose)]
			public void MemoryStreamToArray(Guid guid, string tag, string stack, int size)
			{
				//if (this.IsVerboseEnabled) {
				//	Metrics.Counter("app.memorystream.toarray",size);
				//}
			}

			[Event(6, Level = EventLevel.Informational)]
			public void MemoryStreamManagerInitialized(int blockSize, int largeBufferMultiple, int maximumBufferSize)
			{
				
				//if (this.IsEnabled())
				//{
				//	WriteEvent(6, blockSize, largeBufferMultiple, maximumBufferSize);
				//}
			}

			[Event(7, Level = EventLevel.Verbose)]
			public void MemoryStreamNewBlockCreated(long smallPoolInUseBytes)
			{
				//if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
				//{
				//	WriteEvent(7, smallPoolInUseBytes);
				//}
			}

			[Event(8, Level = EventLevel.Verbose)]
			public void MemoryStreamNewLargeBufferCreated(int requiredSize, long largePoolInUseBytes)
			{
				//if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
				//{
				//	WriteEvent(8, requiredSize, largePoolInUseBytes);
				//}
			}

			[Event(9, Level = EventLevel.Verbose)]
			public void MemoryStreamNonPooledLargeBufferCreated(int requiredSize, string tag, string allocationStack)
			{
				//if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
				//{
				//	WriteEvent(9, requiredSize, tag ?? string.Empty, allocationStack ?? string.Empty);
				//}
			}

			[Event(10, Level = EventLevel.Warning)]
			public void MemoryStreamDiscardBuffer(MemoryStreamBufferType bufferType, string tag,
												  MemoryStreamDiscardReason reason)
			{
				//if (this.IsEnabled())
				//{
				//	WriteEvent(10, bufferType, tag ?? string.Empty, reason);
				//}
			}

			[Event(11, Level = EventLevel.Error)]
			public void MemoryStreamOverCapacity(int requestedCapacity, long maxCapacity, string tag,
												 string allocationStack)
			{
				//if (this.IsEnabled())
				//{
				//	WriteEvent(11, requestedCapacity, maxCapacity, tag ?? string.Empty, allocationStack ?? string.Empty);
				//}
			}
		}
	}
}