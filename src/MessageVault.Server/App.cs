using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using MessageVault.Server.Auth;
using MessageVault.Server.Election;
using Microsoft.Owin.Hosting;
using Nancy;
using Nancy.Authentication.Basic;
using Nancy.Bootstrapper;
using Nancy.Owin;
using Nancy.Security;
using Nancy.TinyIoc;
using Owin;
using Serilog;

namespace MessageVault.Server {

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
			var startOptions = new StartOptions();
			startOptions.Urls.Add(config.InternalUri);
			startOptions.Urls.Add(config.PublicUri);

			var auth = LoadAuth.LoadFromStorageAccount(config.StorageAccount);
			AddSystemAccess(auth, config.StorageAccount.Credentials.ExportBase64EncodedKey());
			var api = ApiImplementation.Create(config.StorageAccount, poller, auth);
			var nancyOptions = new NancyOptions
			{
				Bootstrapper = new NancyBootstrapper(api, new UserValidator(auth))
			};
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

		static void AddSystemAccess(AuthData auth, string accountStorageKey) {
			
			auth.Users.Add(Constants.ClusterNodeUser, new UserInfo {
				Password = accountStorageKey,
				Claims = new [] {"all:write"}
			});
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

		public class UserIdentity : IUserIdentity
		{
			public string UserName { get; set; }

			public IEnumerable<string> Claims { get; set; }
		}
		sealed class UserValidator : IUserValidator {
			readonly AuthData _auth;

			public UserValidator(AuthData auth) {
				_auth = auth;
			}

			public IUserIdentity Validate(string username, string password) {
				// TODO: add bcrypt handling
				UserInfo value;
				if (_auth.Users.TryGetValue(username, out value) && string.Equals(value.Password, password)) {
					return new UserIdentity() {
						UserName = username,
						Claims = value.Claims,
					};
				}
				return null;
			}
		}

		/// <summary>
		///   Passes our dependencies to Nancy modules
		/// </summary>
		sealed class NancyBootstrapper : DefaultNancyBootstrapper {
			readonly ApiImplementation _scheduler;
			readonly IUserValidator _validator;

			public NancyBootstrapper(ApiImplementation scheduler, IUserValidator validator ) {
				_scheduler = scheduler;
				_validator = validator;
			}

			protected override void ConfigureApplicationContainer(TinyIoCContainer container) {
				base.ConfigureApplicationContainer(container);
				container.Register(_scheduler);
				container.Register(_validator);
			}
			protected override void ApplicationStartup(TinyIoCContainer container, IPipelines pipelines)
			{
				base.ApplicationStartup(container, pipelines);

				var configuration = new BasicAuthenticationConfiguration(
					container.Resolve<IUserValidator>(), 
						"MessageVault"
						);
				pipelines.EnableBasicAuthentication(configuration);
			}
		}
	}

}