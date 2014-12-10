using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Nancy;

namespace WorkerRole {

	public sealed class ApiModule : NancyModule {
		readonly ConcurrentExclusiveSchedulerPair _scheduler;
		readonly StreamWriterFactory _factory;
		static CancellationToken _ct;
		static TaskCreationOptions _tco;

		public ApiModule(
			ConcurrentExclusiveSchedulerPair scheduler,
			StreamWriterFactory factory
			) {
			_scheduler = scheduler;
			_factory = factory;
			BuildRoutes();

			//Post["/streams/{id}", true] = Func;
		}

		void BuildRoutes() {
			Get["/"] = x => "hi";

			_ct = CancellationToken.None;
			_tco = TaskCreationOptions.None;


			Get["/streams/{id}", true] =
				(o, token) => Task.Factory.StartNew(() => {
					var id = (string) o.id;

					// reuse buffers?
					// TODO: validation and pre-split
					var mem = new MemoryStream();
					Request.Body.CopyTo(mem);

					return new {id, mem};
				}, _ct, _tco, _scheduler.ConcurrentScheduler).ContinueWith(task => {
					var segment = _factory.Get(task.Result.id);

					var mem = task.Result.mem;
					segment.Append(new[] {mem.ToArray()});
					mem.Dispose();

					dynamic response = "ok";
					return response;
				}, _scheduler.ExclusiveScheduler);
		}
	}
}