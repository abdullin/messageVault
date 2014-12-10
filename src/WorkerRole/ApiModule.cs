using System.IO;
using Nancy;

namespace WorkerRole {

	public sealed class ApiModule : NancyModule {
		readonly StreamScheduler _scheduler;

		public ApiModule(StreamScheduler scheduler) {
			_scheduler = scheduler;
			BuildRoutes();
		}

		void BuildRoutes() {
			Get["/"] = x => "This is MessageVault speaking!";


			Get["/streams/{id}"] = x => {
				var id = (string) x.id;
				var signature = _scheduler.GetReadAccessSignature(id);
				return Response.AsJson(new {
					signature = signature
				});
			};
			Post["/streams/{id}", true] = async (x, ct) => {
				var mem = new MemoryStream();
				Request.Body.CopyTo(mem);
				var id = (string) x.id;


				var pos = await _scheduler.Append(id, new[] {mem.ToArray()});
				return Response.AsJson(new {
					position = pos
				});
			};
		}
	}

}