using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MessageVault.Election;
using Microsoft.Owin.Hosting;
using Microsoft.WindowsAzure.Storage;
using Nancy;
using Nancy.Owin;
using Nancy.TinyIoc;
using Owin;

namespace WorkerRole {

	public sealed class AppConfig {
		public  string PublicUri;
		public  string InternalUri;
		public CloudStorageAccount StorageAccount;

		public void ThrowIfInvalid() {
			if (string.IsNullOrWhiteSpace(PublicUri)) {
				throw new InvalidOperationException("Specify public uri");
			}
			if (string.IsNullOrWhiteSpace(InternalUri)) {
				throw new InvalidOperationException("Specify private uri");
			}
			if (StorageAccount == null) {
				throw new InvalidOperationException("Storage account must be specified");
			}
		}
	}

	public sealed class App {
		readonly IDisposable _api;
		readonly CancellationTokenSource _source = new CancellationTokenSource();

		App(IDisposable api, CancellationTokenSource source, IList<Task> tasks) {
			_api = api;
			_source = source;
			_tasks = tasks;
		}

		readonly IList<Task> _tasks;

		public static App Initialize(AppConfig config) {
			config.ThrowIfInvalid();
			

			var scheduler = StreamScheduler.CreateDev();

			var nancyOptions = new NancyOptions {
				Bootstrapper = new NancyBootstrapper(scheduler)
			};
			var startOptions = new StartOptions();
			startOptions.Urls.Add(config.InternalUri);
			startOptions.Urls.Add(config.PublicUri);

			var nodeInfo = new NodeInfo(config.InternalUri);
			var leader = new LeaderSelector(config.StorageAccount, nodeInfo);
			

			
			var cts = new CancellationTokenSource();
			// fire up leader and scheduler first
			var tasks = new List<Task> {
				leader.Run(cts.Token), 
				scheduler.Run(cts.Token)
			};
			// bind the API
			var api = WebApp.Start(startOptions, x => x.UseNancy(nancyOptions));
			return new App(api, cts, tasks);
		}



		public void RequestStop() {
			// signal stopping for all
			_source.Cancel();
			// kill api and stop accepting new requests
			_api.Dispose();

		}

		public Task GetCompletionTask() {
			return Task.WhenAll(_tasks);
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