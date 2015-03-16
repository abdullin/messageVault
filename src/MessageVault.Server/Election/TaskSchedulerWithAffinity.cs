using System;
using System.Threading.Tasks;
using System.Linq;

namespace MessageVault.Server.Election {

	/// <summary>
	/// Allows multiple tasks to run in parallel. However, tasks
	/// with the same hash will always run sequentially.
	/// </summary>
	public sealed class TaskSchedulerWithAffinity {
		readonly SequentialJobScheduler[] _schedulers;

		public TaskSchedulerWithAffinity(int parallelism) {
			
			_schedulers = new SequentialJobScheduler[parallelism];
			for (int i = 0; i < parallelism; i++)
			{
				_schedulers[i] = new SequentialJobScheduler();
			}
		}

		public Task<T> StartNew<T>(int hash, Func<T> call) {
			var id = ((uint) hash) % _schedulers.Length;
			var thread = _schedulers[id];
			return thread.Factory.StartNew(call);
		}

		public  Task Shutdown() {
			foreach (var job in _schedulers) {
				job.Scheduler.Complete();
			}
			var completes = _schedulers.Select(s => s.Scheduler.Completion);
			return Task.WhenAll(completes);
		}

		sealed class SequentialJobScheduler {
			internal readonly ConcurrentExclusiveSchedulerPair Scheduler;
			internal readonly TaskFactory Factory;

			public SequentialJobScheduler() {
				Scheduler = new ConcurrentExclusiveSchedulerPair();
				Factory = new TaskFactory(Scheduler.ExclusiveScheduler);
			}
		}
	}
}