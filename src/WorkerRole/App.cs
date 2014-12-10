using System;
using System.Threading.Tasks;
using Microsoft.Owin.Hosting;
using Nancy;
using Nancy.Owin;
using Nancy.TinyIoc;
using Owin;

namespace WorkerRole {

	public sealed class App {
		readonly IDisposable _api;
		readonly StreamScheduler _scheduler;


		App(IDisposable api, StreamScheduler scheduler) {
			_api = api;
			_scheduler = scheduler;
		}

		public static App Initialize(string baseUri) {
			var scheduler = StreamScheduler.CreateDev();

			var nancyOptions = new NancyOptions {
				Bootstrapper = new NancyBootstrapper(scheduler)
			};
			var api = WebApp.Start(baseUri, x => x.UseNancy(nancyOptions));
			return new App(api, scheduler);
		}


		public void RequestStop() {
			// stop accepting new requests
			_api.Dispose();

			
			_scheduler.RequestShutdown();
		}

		public Task GetCompletionTask() {
			return _scheduler.GetCompletionTask();
		}

		/// <summary>
		///   Passes our dependencies to Nancy modules
		/// </summary>
		sealed class NancyBootstrapper : DefaultNancyBootstrapper {
			readonly StreamScheduler _scheduler;

			public NancyBootstrapper(StreamScheduler scheduler) {
				_scheduler = scheduler;
			}

			protected override void ConfigureApplicationContainer(TinyIoCContainer container) {
				container.Register(_scheduler);
				base.ConfigureApplicationContainer(container);
			}
		}
	}

}