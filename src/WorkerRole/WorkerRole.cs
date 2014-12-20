using System;
using System.Net;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
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

			var config = new AppConfig {
				InternalUri = GetEndpointAsUri("InternalHttp"),
				PublicUri = GetEndpointAsUri("Http"),
				StorageAccount = CloudStorageAccount.DevelopmentStorageAccount
			};
			_app = App.Initialize(config);

			return base.OnStart();
		}

		static string GetEndpointAsUri(string name) {
			var http = RoleEnvironment.CurrentRoleInstance.InstanceEndpoints[name];
			var baseUri = String.Format("{0}://{1}", http.Protocol, http.IPEndpoint);
			return baseUri;
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