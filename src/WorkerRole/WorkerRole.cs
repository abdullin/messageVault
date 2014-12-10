using System;
using System.Net;
using Microsoft.WindowsAzure.ServiceRuntime;
using Nancy;
using Serilog;

namespace WorkerRole {

	public class WorkerRole : RoleEntryPoint {
		public override void Run() {
			_app.GetCompletionTask()
			    .Wait();
		}


		App _app;

		public override bool OnStart() {
			//Logging. configure
			ServicePointManager.DefaultConnectionLimit = 12;
			var endpoint = RoleEnvironment.CurrentRoleInstance.InstanceEndpoints["Http"];
			var baseUri = String.Format("{0}://{1}", endpoint.Protocol, endpoint.IPEndpoint);
			_app = App.Create(baseUri);

			return base.OnStart();
		}

		public override void OnStop() {
			_app.RequestStop();
			Log.Information("Waiting to stop");

			var shutdownTimeout = TimeSpan.FromSeconds(10);
			var task = _app.GetCompletionTask();
			if (!task.Wait(shutdownTimeout)) {
				Log.Information("Forcing shutdown soon");
				task.Wait(TimeSpan.FromSeconds(5));
			}
		}
	}


	

}