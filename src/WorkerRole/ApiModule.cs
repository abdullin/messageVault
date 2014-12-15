using System;
using System.IO;
using Microsoft.WindowsAzure.Storage;
using Nancy;
using Serilog;

namespace WorkerRole {

	public sealed class ApiModule : NancyModule {
		readonly StreamScheduler _scheduler;

		public ApiModule(StreamScheduler scheduler) {
			_scheduler = scheduler;
			BuildRoutes();
		}


		Response WrapException(Exception ex) {
			var se = ex as StorageException;

			if (se != null) {
				return Response.AsJson(new {
					error = se.Message,
					type = "storage",
				}, HttpStatusCode.InternalServerError);
			}
			return Response.AsJson(new {error = ex.Message,}, HttpStatusCode.InternalServerError);
		}

		void BuildRoutes() {

			Before += ctx => {
				Log.Debug("{method} {url}", ctx.Request.Method, ctx.Request.Path);
				return null;
			};
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

				try {
					var pos = await _scheduler.Append(id, new[] {mem.ToArray()});
					return Response.AsJson(new {
						position = pos
					});
				}
				catch (Exception ex) {
					return WrapException(ex);
				}
				
			};
		}
	}
}