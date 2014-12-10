using System;
using System.Threading.Tasks;
using Microsoft.Owin.Hosting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Nancy;
using Nancy.Owin;
using Nancy.TinyIoc;
using Owin;

namespace WorkerRole {

	public sealed class App{
		readonly IDisposable _api;
		readonly ConcurrentExclusiveSchedulerPair _scheduler;
		readonly StreamWriterFactory _streams;
		readonly Task _completionTask;


		App(IDisposable api, ConcurrentExclusiveSchedulerPair scheduler, StreamWriterFactory streams, Task completionTask) {
			_api = api;
			_scheduler = scheduler;
			_streams = streams;
			_completionTask = completionTask;
		}

		public static App Create(string baseUri) {
			var scheduler = new ConcurrentExclusiveSchedulerPair();
			var streams = StreamWriterFactory.CreateDev();
			
			var nancyOptions = new NancyOptions {
				Bootstrapper = new NancyBootstrapper(scheduler, streams)
			};
			var app = WebApp.Start(baseUri, x => x.UseNancy(nancyOptions));


			// after scheduler cleans up tasks, we dispose streams
			var completionTask = scheduler.Completion
				.ContinueWith(task => streams.Dispose());

			return new App(app, scheduler, streams, completionTask);
		}



		

		public void RequestStop() {
			// stop accepting new requests
			_api.Dispose();
			// tell scheduler to stop accepting new tasks
			_scheduler.Complete();
		}

		public Task GetCompletionTask() {
			return _completionTask;
		}

		/// <summary>
		/// Passes our dependencies to Nancy modules
		/// </summary>
		sealed class NancyBootstrapper : DefaultNancyBootstrapper
		{
			readonly ConcurrentExclusiveSchedulerPair _scheduler;
			readonly StreamWriterFactory _streams;

			public NancyBootstrapper(
					ConcurrentExclusiveSchedulerPair scheduler,
				StreamWriterFactory streams)
			{
				_scheduler = scheduler;
				_streams = streams;
			}

			protected override void ConfigureApplicationContainer(TinyIoCContainer container) {
				container.Register(_scheduler);
				container.Register(_streams);
				//container.Re
				base.ConfigureApplicationContainer(container);
			}
		}

	}

}