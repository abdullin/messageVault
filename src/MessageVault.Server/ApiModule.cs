using System;
using MessageVault.Api;
using MessageVault.Server.Election;
using Microsoft.WindowsAzure.Storage;
using Nancy;
using Nancy.Security;

namespace MessageVault.Server {

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
			Get["/"] = x => "This is MessageVault speaking!";


			Get["/streams/{id}"] = x => {
				this.RequiresAuthentication();
				var id = (string) x.id;
				RequiresReadAccess(id);

				
				var response = _scheduler.GetReadAccess(id);
				return Response.AsJson(response);
			};
			Post["/streams/{id}", true] = async (x, ct) => {

				this.RequiresAuthentication();
				var id = (string) x.id;
				RequiresWriteAccess(id);
				
				try {
					var md5 = Request.Query["md5"];
					byte[] hash = null;

					if (md5 != null) {
						hash = Guid.Parse((string) md5).ToByteArray();
					}
					// read messages in request thread
					var messages = ApiMessageFramer.ReadMessages(Request.Body, hash);
					var response = await _scheduler.Append(id, messages);

					return Response.AsJson(response);
				}
				catch (Exception ex) {
					Serilog.Log.Error(ex, "Failure in POST to {id}", id);
					return WrapException(ex);
				}
			};
		}

		void RequiresReadAccess(string id) {
			this.RequiresAnyClaim(new[] {"all:read", "all:write", id + ":read", id + ":write"});
		}

		void RequiresWriteAccess(string id) {
			this.RequiresAnyClaim(new[] {"all:write", id + ":write"});
		}
	}

}