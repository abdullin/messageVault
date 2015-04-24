using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MessageVault;
using MessageVault.Api;

namespace StressTester {

	class Program {
		static void Main(string[] args) {
			var threads = GetIntParam(args, "threads", 1);
			var url = GetStringParam(args, "url", "http://localhost:8001");
			var pass = GetStringParam(args, "pass", Constants.DefaultPassword);
			var login = GetStringParam(args, "login", Constants.DefaultLogin);
			var steps = GetIntParam(args, "steps", 100);
			var batchSize = GetIntParam(args, "batch", 100);

			Console.WriteLine(new {threads, url, pass, login, steps, batchSize});

			var client = new Client(url, login, pass);
			var list = new List<Task>();
			var cts = new CancellationTokenSource();

			for (var i = 0; i < threads; i++) {
				var taskId = i;
				
				list.Add(RunNonCompetingPush(taskId, steps, batchSize, client, cts.Token).ContinueWith(task => {
					if (task.IsFaulted) {
						Console.WriteLine(task.Exception);
					}
				}));
			}

			list.Add(Task.Factory.StartNew(() => {
				while (!cts.Token.IsCancellationRequested) {
					cts.Token.WaitHandle.WaitOne(5000);
					PrintStats();
				}
			}));

			Console.WriteLine("Waiting for tasks to stop");
			Task.WaitAll(Enumerable.ToArray(list));
			PrintStats();

		}

		static void PrintStats() {
			var secondsSpent = MillisecondsPosting / 1000;
			Console.WriteLine("{0} mps / {1} tps. Total waited {2} sec, sent {3} in {4} batches",
				MessagesPosted / secondsSpent,
				BatchesPosted / secondsSpent,
				secondsSpent,
				MessagesPosted,
				BatchesPosted);
		}

		static byte[] GenerateMessage(int rand) {
			var size = new byte[(rand % 1021) + 117];
			var rng = new Random(rand);
			rng.NextBytes(size);
			return size;
		}

		static long MessagesPosted;
		static long BatchesPosted;
		static long MillisecondsPosting;

		static async Task RunNonCompetingPush(int i, int steps, int batchSize, Client client, CancellationToken token) {

			var streamName = "test" + i;
			var reader = await client.GetMessageReaderAsync(streamName);

			var starting = reader.GetPosition();

			
			for (int j = 0; j < steps; j++) {

				if (token.IsCancellationRequested) {
					return;
				}
				var messages = new List<MessageToWrite>();
				for (int k = 0; k < batchSize; k++) {
					var contract = string.Format("message{0}-{1}", j, k);
					var seq = j * batchSize + k;
					messages.Add(new MessageToWrite(0, contract, GenerateMessage(seq)));
				}
				var started = Stopwatch.StartNew();
				await client.PostMessagesAsync(streamName, messages);
				// time
				Interlocked.Increment(ref BatchesPosted);
				Interlocked.Add(ref MessagesPosted, batchSize);
				Interlocked.Add(ref MillisecondsPosting, started.ElapsedMilliseconds);

			}
			Console.WriteLine("Task {0} done writing", i);


			long position = starting;
			for (int j = 0; j < (steps); j++) {
				var result = await reader.GetMessagesAsync(token, position, batchSize);
				
				for (int k = 0; k < batchSize; k++) {
					var seq = j * batchSize + k;

					var msg = result.Messages[k];
					var expect = string.Format("message{0}-{1}", j, k);
					var body = GenerateMessage(seq);
					if (msg.Key != expect) {
						throw new InvalidOperationException("Unexpected contract");
					}
					if (!body.SequenceEqual(msg.Value)) {
						throw new InvalidOperationException("Unexpected data");
					}
				}

				position = result.NextOffset;
			}
			Console.WriteLine("Task {0} done checking", i);
		}


		static int GetIntParam(string[] args, string prefix, int def) {
			var str = GetStringParam(args, prefix, null);
			return str == null ? def : int.Parse(str);
		}

		static string GetStringParam(string[] args, string prefix, string def) {
			var full = prefix + "=";
			var value = args.FirstOrDefault(s => s.StartsWith(full));
			return value == null ? def : value.Replace(full, "").Trim();
		}
	}

}