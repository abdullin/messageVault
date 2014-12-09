using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Blob;
using Serilog;

namespace MessageVault {

    public class AzureReader {
        CloudPageBlob _blob;



        public long GetCheckpoint() {
            _blob.FetchAttributes();

            string check;
            if (!_blob.Metadata.TryGetValue("checkpoint", out check)) {
                return 0;
            }
            return long.Parse(check);
        }


    }

    public class AzureSegment {
        readonly CloudPageBlob _blob;
        public long BlobSize { get; private set; }
        public long BlobPosition { get; private set; }


        // 4MB, Azure limit
        const long CommitSizeBytes = 1024*1024*4;
        // this would allow consumers to use fixed-size buffers.
        // Also see Snappy framing format
        // https://code.google.com/p/snappy/source/browse/trunk/framing_format.txt
        const long MaxMessageSize = 65536;
        // Azure limit
        const int PageSize = 512;

        readonly byte[] _buffer = new byte[CommitSizeBytes];
        readonly MemoryStream _stream;

        

        static long NextSize(long current) {
            // Azure doesn't charge us for the page storage anyway
            const long hundredMBs = 1024*1024*100;
            return current + hundredMBs;
        }

        public AzureSegment(CloudPageBlob blob) {
            _blob = blob;

            _stream = new MemoryStream(_buffer, true);
        }

        public void InitNew() {
            var nextSize = NextSize(BlobSize);
            _blob.Metadata["position"] = "0";
            _blob.Create(nextSize);
            BlobSize = nextSize;
        }

        public void OpenExisting() {
            //_blob.FetchAttributes();

            BlobSize = _blob.Properties.Length;

            string position;
            if (_blob.Metadata.TryGetValue("position", out position)) {
                BlobPosition = long.Parse(position);

                var hasTail = BlobPosition%PageSize != 0;
                if (hasTail) {
                    _blob.DownloadRangeToStream(_stream, PageFloor(BlobPosition), PageSize);
                    _stream.Seek(BlobPosition, SeekOrigin.Begin);
                }
            }
            else {
                throw new InvalidOperationException("Blob was expected to have position");
            }

        }

        void GrowBlob() {
            var nextSize = NextSize(BlobSize);
            _blob.Resize(nextSize);
            BlobSize = nextSize;
        }

        static long PagesCeiling(long value) {
            var tail = value%PageSize;
            if (tail == 0) {
                return value;
            }
            return value - tail + PageSize;
        }

        static long PageFloor(long value) {
            var tail = value%PageSize;
            return value - tail;
        }


        void FlushBuffer() {

            
            var bytesToWrite = _stream.Position;

            Log.Verbose("Flush buffer with {size} at {position}", bytesToWrite, BlobPosition);

            var tail = bytesToWrite%PageSize;
            var writeSize = PagesCeiling(bytesToWrite);

            if (PagesCeiling(BlobPosition + bytesToWrite) > BlobSize) {
                GrowBlob();
            }


            using (var copy = new MemoryStream(_buffer, 0, (int) writeSize)) {
                _blob.WritePages(copy, 0);
            }

            BlobPosition += bytesToWrite;


            if (tail == 0) {
                Array.Clear(_buffer, 0, _buffer.Length);
                _stream.Seek(0, SeekOrigin.Begin);
                return;
            }

            if (writeSize < PageSize) {
                // nothing to clear
                return;
            }

            var starts = writeSize - PageSize;
            // copy tail to the beginning of the buffer
            Array.Copy(_buffer, starts, _buffer, 0, PageSize);
            // clear the rest
            Array.Clear(_buffer, PageSize, _buffer.Length - PageSize);
            _stream.Seek(tail, SeekOrigin.Begin);
        }


        public void Append(IEnumerable<byte[]> data) {
            foreach (var chunk in data) {
                if (chunk.Length > MaxMessageSize) {
                    var message = "Each message must be smaller than " + MaxMessageSize;
                    throw new InvalidOperationException(message);
                }

                var newBlock = 4 + chunk.Length;
                if (newBlock + _stream.Position >= _stream.Length) {
                    FlushBuffer();
                }
                _stream.Write(BitConverter.GetBytes(chunk.Length), 0, 4);
                _stream.Write(chunk, 0, chunk.Length);
            }
            FlushBuffer();


        }
    }


    public class AzureSegmentFactory {
        readonly CloudBlobContainer _client;

        public AzureSegmentFactory(CloudBlobContainer client) {
            _client = client;
        }


        public AzureSegment OpenOrCreate(string stream) {
            
            var blob = _client.GetPageBlobReference(stream);
            var segment = new AzureSegment(blob);
            if (!blob.Exists()) {
                segment.InitNew();
            }
            else {
                segment.OpenExisting();
            }

            return segment;
            
        }
    }

}