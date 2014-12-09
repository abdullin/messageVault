using System;
using System.Diagnostics.Contracts;
using System.IO;

namespace MessageVault {
    /// <summary>
    ///     Helps to write data to the underlying store, which accepts only
    ///     pages with specific size
    /// </summary>
    sealed class PageWriter : IDisposable {
        /// <summary>
        ///     Delegate that writes pages to the underlying paged store.
        /// </summary>
        /// <param name="offset">The offset.</param>
        /// <param name="source">The source.</param>
        public delegate void AppendWriterDelegate(int offset, Stream source);

        public delegate byte[] TailLoaderDelegate(long position, int count);

        int _bytesPending;
        bool _disposed;
        int _fullPagesFlushed;
        MemoryStream _pending;
        readonly int _pageSizeInBytes;
        readonly AppendWriterDelegate _writer;

        public PageWriter(int pageSizeInBytes, AppendWriterDelegate writer) {
            _writer = writer;

            _pageSizeInBytes = pageSizeInBytes;
            _pending = new MemoryStream();
        }

        public void Dispose() {
            if (_disposed) {
                return;
            }

            Flush();

            var t = _pending;
            _pending = null;
            _disposed = true;

            t.Dispose();
        }

        public void CacheLastPageIfNeeded(long position, TailLoaderDelegate loader) {
            Contract.Requires(position >= 0);
            Contract.Requires(loader != null);

            if (position == 0) {
                return;
            }
            var total = (int) (position/_pageSizeInBytes);
            var remainder = (int) (position%_pageSizeInBytes);

            _fullPagesFlushed = total;
            if (remainder != 0) {
                // we need to preload data
                _bytesPending = remainder;
                var tip = loader(position - remainder, remainder);
                _pending.Write(tip, 0, remainder);
            }
        }

        public void Write(byte[] buffer) {
            CheckNotDisposed();

            _pending.Write(buffer, 0, buffer.Length);
            _bytesPending += buffer.Length;
        }

        public void Write(byte[] buffer, int offset, long length) {
            CheckNotDisposed();

            _pending.Write(buffer, 0, buffer.Length);
            _bytesPending += (int) length;
        }

        public void Flush() {
            CheckNotDisposed();

            if (_bytesPending == 0) {
                return;
            }

            var size = (int) _pending.Length;
            var padSize = (_pageSizeInBytes - size%_pageSizeInBytes)%_pageSizeInBytes;

            using (var stream = new MemoryStream(size + padSize)) {
                stream.Write(_pending.ToArray(), 0, (int) _pending.Length);
                if (padSize > 0) {
                    stream.Write(new byte[padSize], 0, padSize);
                }

                stream.Position = 0;
                _writer(_fullPagesFlushed*_pageSizeInBytes, stream);
            }

            var fullPagesFlushed = size/_pageSizeInBytes;

            if (fullPagesFlushed <= 0) {
                return;
            }

            // Copy remainder to the new stream and dispose the old stream
            var newStream = new MemoryStream();
            _pending.Position = fullPagesFlushed*_pageSizeInBytes;
            _pending.CopyTo(newStream);
            _pending.Dispose();
            _pending = newStream;

            _fullPagesFlushed += fullPagesFlushed;
            _bytesPending = 0;
        }

        void CheckNotDisposed() {
            if (_disposed) {
                throw new ObjectDisposedException(GetType().FullName);
            }
        }

        public void Reset() {
            _pending.SetLength(0);
            _bytesPending = 0;
            _fullPagesFlushed = 0;
        }
    }
}