using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MessageVault.Election;
using Microsoft.Owin.Hosting;
using Nancy;
using Nancy.Owin;
using Nancy.TinyIoc;
using Owin;
using Serilog;

namespace WorkerRole {

	public sealed class App {
		readonly IDisposable _api;
		readonly ILogger _log = Log.ForContext<App>();
		readonly CancellationTokenSource _source = new CancellationTokenSource();

		App(IDisposable api, CancellationTokenSource source, IList<Task> tasks) {
			_api = api;
			_source = source;
			_tasks = tasks;
		}

		readonly IList<Task> _tasks;

		public static App Initialize(AppConfig config) {
			config.ThrowIfInvalid();
			var poller = LeaderInfoPoller.Create(config.StorageAccount);
			var api = ApiImplementation.Create(config.StorageAccount, poller);
			var nancyOptions = new NancyOptions {
				Bootstrapper = new NancyBootstrapper(api)
			};
			var startOptions = new StartOptions();
			startOptions.Urls.Add(config.InternalUri);
			startOptions.Urls.Add(config.PublicUri);

			var nodeInfo = new LeaderInfo(config.InternalUri);
			
			var selector = new LeaderLock(config.StorageAccount, nodeInfo, api);

			
			
			var cts = new CancellationTokenSource();
			// fire up leader and scheduler first
			var tasks = new List<Task> {
				selector.KeepTryingToAcquireLock(cts.Token), 
				poller.KeepPollingForLeaderInfo(cts.Token),
			};
			// bind the API
			var webApp = WebApp.Start(startOptions, x => x.UseNancy(nancyOptions));
			return new App(webApp, cts, tasks);
		}



		public void RequestStop() {
			// signal stopping for all
			_source.Cancel();
			// kill api and stop accepting new requests
			_api.Dispose();

		}

		public async Task GetCompletionTask() {
			var allTasks = Task.WhenAll(_tasks);
			try {
				await allTasks;
			}
			catch (Exception)
			{
				if (allTasks.Exception != null)
				{
					allTasks.Exception.Handle(ex =>
					{
						if (!(ex is OperationCanceledException))
						{
							_log.Error(ex, "Failure on cancel");
						}

						return true;
					});
				}
			}
		}

		/// <summary>
		///   Passes our dependencies to Nancy modules
		/// </summary>
		sealed class NancyBootstrapper : DefaultNancyBootstrapper {
			readonly ApiImplementation _scheduler;

			public NancyBootstrapper(ApiImplementation scheduler) {
				_scheduler = scheduler;
			}

			protected override void ConfigureApplicationContainer(TinyIoCContainer container) {
				base.ConfigureApplicationContainer(container);
				container.Register(_scheduler);
			}
		}
	}

}