using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.IO;
using System.Text;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Blob.Protocol;
using Serilog;

namespace MessageVault
{
    /// <summary>
    /// Represents collection of events within a Windows Azure Blob 
    /// (residing inside a <see cref="CloudPageBlob"/>). It can be opened as 
    /// mutable or as read-only.
    /// </summary>
    public class OldAzureSegment : IDisposable
    {
        readonly CloudPageBlob _blob;
        readonly PageWriter _pageWriter;
        long _chunkContentSize;
        long _blobSpaceSize;

        public const long MaxCommitSize = 1024 * 1024 * 4;

        


        OldAzureSegment(CloudPageBlob blob, long offset, long size)
        {
            _blob = blob;
            _pageWriter = new PageWriter(512, WriteProc);
            _chunkContentSize = offset;
            _blobSpaceSize = size;

            if (offset > 0)
            {
                _pageWriter.CacheLastPageIfNeeded(offset, BufferTip);
            }
        }

        byte[] BufferTip(long position, int count)
        {
            var buffer = new byte[count];
            using (var s = _blob.OpenRead())
            {
        
                s.Seek(position, SeekOrigin.Begin);
                s.Read(buffer, 0, count);
                return buffer;
            }
        }

        public static OldAzureSegment OpenExistingForWriting(CloudPageBlob blob, long offset, long length)
        {
            Contract.Requires(length>0);
            Contract.Requires(offset>=0);
            
            return new OldAzureSegment(blob, offset, length);
        }

        public static OldAzureSegment CreateNewForWriting(CloudPageBlob blob)
        {
            blob.Create(MaxCommitSize);
            return new OldAzureSegment(blob, 0, MaxCommitSize);
        }
        public static OldAzureSegment OpenExistingForReading(CloudPageBlob blob, long length)
        {
            Contract.Requires(length>0);
            return new OldAzureSegment(blob, -1, length);
        }

        public ChunkAppendResult Append(string streamId, IEnumerable<byte[]> eventData)
        {
            const int limit = 4 * 1024 * 1024 - 1024; // mind the 512 boundaries
            long writtenBytes = 0;
            int writtenEvents = 0;
            using (var bufferMemory = new MemoryStream())
            using (var bufferWriter = new BinaryWriter(bufferMemory))
            {
                foreach (var record in eventData)
                {
                    var newSizeEstimate = 4 + Encoding.UTF8.GetByteCount(streamId) + 4 + record.Length;
                    if (bufferMemory.Position + newSizeEstimate >= limit)
                    {
                        bufferWriter.Flush();
                        _pageWriter.Write(bufferMemory.ToArray(), 0, bufferMemory.Position);
                        _pageWriter.Flush();
                        writtenBytes += bufferMemory.Position;
                        bufferMemory.Seek(0, SeekOrigin.Begin);
                    }

                    bufferWriter.Write(streamId);
                    bufferWriter.Write((int)record.Length);
                    bufferWriter.Write(record);
                    writtenEvents += 1;
                }
                bufferWriter.Flush();
                _pageWriter.Write(bufferMemory.ToArray(), 0, bufferMemory.Position);
                _pageWriter.Flush();
                writtenBytes += bufferMemory.Position;
            }
            _chunkContentSize += writtenBytes;

            return new ChunkAppendResult(writtenBytes, writtenEvents, _chunkContentSize);
        }


        public void Reset()
        {
            _pageWriter.Reset();
            _chunkContentSize = 0;
        }

        void WriteProc(int offset, Stream source)
        {
            if (!source.CanSeek)
                throw new InvalidOperationException("Seek must be supported by a stream.");

            var length = source.Length;
            if (offset + length > _blobSpaceSize)
            {
                var newSize = _blobSpaceSize + MaxCommitSize;
                Log.Debug("Increasing chunk size to {NewSize}", newSize);
                _blob.Resize(newSize);
                
                
                _blobSpaceSize = newSize;
            }

            _blob.WritePages(source, offset);
        }

        


      

        public void Dispose()
        {

        }
    }



    /// <summary>
    /// Result of appending events to a chunk. It will be a failure,
    /// if we are overflowing chunk boundaries and need to write
    /// to a new one. 
    /// </summary>
    public struct ChunkAppendResult
    {
        public readonly long WrittenBytes;
        public readonly int WrittenEvents;
        public readonly long ChunkPosition;

        public ChunkAppendResult(long writtenBytes, int writtenEvents, long chunkPosition)
        {
            WrittenBytes = writtenBytes;
            WrittenEvents = writtenEvents;
            ChunkPosition = chunkPosition;
        }
    }
}