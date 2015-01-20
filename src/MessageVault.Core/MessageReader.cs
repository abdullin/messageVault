using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace MessageVault {

    public sealed class MessageReader {
        readonly ICheckpointReader _position;
        readonly IPageReader _messages;

        readonly byte[] _buffer;
        const int Limit = 1024*1024*4;

        public MessageReader(ICheckpointReader position, IPageReader messages) {
            _position = position;
            _messages = messages;
            _buffer = new byte[Limit];
        }


        public long GetPosition() {
            return _position.Read();
        }

        public MessageResult ReadMessages(long from, long till, int maxCount) {
            Require.ZeroOrGreater("from", from);
            Require.ZeroOrGreater("maxOffset", till);
            Require.Positive("maxCount", maxCount);

            var list = new List<Message>(maxCount);
            var position = from;

            using (var prs = new PageReadStream(_messages, from, till, _buffer)) {
                using (var bin = new BinaryReader(prs)) {
                    while (prs.Position < prs.Length) {
                        var message = MessageFormat.Read(bin);
                        list.Add(message);
                        position = prs.Position;
                        if (list.Count >= maxCount) {
                            break;
                        }
                    }
                }
            }
            return new MessageResult(list, position);
        }


        public async Task<MessageResult> GetMessagesAsync(CancellationToken ct, long start,
            int limit) {
            while (!ct.IsCancellationRequested) {
                var actual = _position.Read();
                if (actual < start) {
                    var msg = string.Format("Actual stream length is {0}, but requested {1}", actual,
                        start);
                    throw new InvalidOperationException(msg);
                }
                if (actual == start) {
                    await Task.Delay(1000, ct);
                    continue;
                }
                var result = await Task.Run(() => ReadMessages(start, actual, limit));

                return result;
            }
            return MessageResult.Empty(start);
        }
    }

}