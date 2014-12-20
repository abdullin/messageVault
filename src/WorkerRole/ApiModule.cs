using System;
using MessageVault.Api;
using MessageVault.Election;
using Microsoft.WindowsAzure.Storage;
using Nancy;
using Serilog;

namespace WorkerRole {

	public sealed class ApiModule : NancyModule {
		readonly ApiImplementation _scheduler;

		public ApiModule(ApiImplementation scheduler) {
			_scheduler = scheduler;
			BuildRoutes();
		}


		Response WrapException(Exception ex) {
			var se = ex as StorageException;

			if (se != null) {
				return Response.AsJson(new ErrorResponse {
					Error = se.Message,
					Type = "storage",
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
				var response = _scheduler.GetReadAccess(id);
				return Response.AsJson(response);

			};
			Post["/streams/{id}", true] = async (x, ct) => {
				// read messages in request thread
				var messages = ApiMessageFramer.ReadMessages(Request.Body);
				var id = (string) x.id;

				try {
					var response = await _scheduler.Append(id, messages);

					return Response.AsJson(response);
				}
				catch (Exception ex) {
					return WrapException(ex);
				}
			};
		}
	}

}