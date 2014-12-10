using System;
using System.Diagnostics;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Owin.Hosting;
using Microsoft.WindowsAzure.ServiceRuntime;
using Nancy;
using Nancy.Owin;
using Owin;

namespace WorkerRole {

	public class WorkerRole : RoleEntryPoint {

		public override void Run() {
			Trace.TraceInformation("WorkerRole is running");
			while (true) {
				Thread.Sleep(1000);
			}
		}


		IDisposable _app;

		public override bool OnStart() {
			//Logging. configure
			ServicePointManager.DefaultConnectionLimit = 12;


			var endpoint = RoleEnvironment.CurrentRoleInstance.InstanceEndpoints["Http"];
			var baseUri = String.Format("{0}://{1}", endpoint.Protocol, endpoint.IPEndpoint);

			Trace.TraceInformation(String.Format("Starting OWIN at {0}", baseUri), "Information");
				_app = WebApp.Start<Startup>(new StartOptions(url : baseUri));
			
			return base.OnStart();
		}

		public override void OnStop() {
			_app.Dispose();
			base.OnStop();
		}

	}


	public class Startup
	{
		public void Configuration(IAppBuilder app)
		{
			app.UseNancy(new NancyOptions { });
		}
	}


	static  class Global {
		
	}

	public class Mod : NancyModule
	{
		public Mod()
		{
			Get["/"] = x => "hi";

			//Post["/streams/{id}", true] = Func;
		}

		
	}

}