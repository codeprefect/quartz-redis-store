using Common.Logging;
using Quartz;
using Quartz.Impl.Matchers;
using Quartz.Spi;
using StackExchange.Redis;

namespace QuartzRedis.Store
{
    /// <summary>
    /// Redis Job Store
    /// </summary>
    public class RedisJobStore : IJobStore
    {

        #region private fields
        /// <summary>
        /// logger
        /// </summary>
        private readonly ILog _logger = LogManager.GetLogger(typeof(RedisJobStore));
        /// <summary>
        /// redis job store schema
        /// </summary>
        private RedisJobStoreSchema _storeSchema;
        /// <summary>
        /// redis db.
        /// </summary>
        private IDatabase _db;
        /// <summary>
        /// master/slave redis store.
        /// </summary>
        private RedisStorage _storage;

        /// <summary>
        /// periodically sweeps for orphaned trigger locks, independent of the main store lock so that
        /// cleanup can never be starved by unrelated store traffic (see <see cref="BaseJobStorage.ReleaseTriggers"/>).
        /// </summary>
        private Timer _orphanCleanupTimer;

        #endregion

        #region public properties

        /// <summary>
        /// Indicates whether job store supports persistence.
        /// </summary>
        /// <returns/>
        public bool SupportsPersistence
        {
            get { return true; }
        }
        /// <summary>
        /// How long (in milliseconds) the <see cref="T:Quartz.Spi.IJobStore"/> implementation
        ///             estimates that it will take to release a trigger and acquire a new one.
        /// </summary>
        public long EstimatedTimeToReleaseAndAcquireTrigger
        {
            get { return 200; }
        }
        /// <summary>
        /// Whether or not the <see cref="T:Quartz.Spi.IJobStore"/> implementation is clustered.
        /// </summary>
        /// <returns/>
        public bool Clustered { get; set; }
        //{
        //    get { return true; }
        //}
        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> of the Scheduler instance's Id,
        ///             prior to initialize being invoked.
        /// </summary>
        public string InstanceId { get; set; }
        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> of the Scheduler instance's name,
        ///             prior to initialize being invoked.
        /// </summary>
        public string InstanceName { get; set; }
        /// <summary>
        /// Tells the JobStore the pool size used to execute jobs.
        /// </summary>
        public int ThreadPoolSize { get; set; }
        /// <summary>
        /// Redis configuration
        /// </summary>
        public string RedisConfiguration { set; get; }

        /// <summary>
        /// gets / sets the delimiter for concatinate redis keys.
        /// </summary>
        public string KeyDelimiter { get; set; }

        /// <summary>
        /// gets /sets the prefix for redis keys.
        /// </summary>
        public string KeyPrefix { get; set; }

        /// <summary>
        /// trigger lock time out, used to release the orphan triggers in case when a scheduler crashes and still has locks on some triggers.
        /// make sure the lock time out is bigger than the time for running the longest job.
        /// </summary>
        public int? TriggerLockTimeout { get; set; }

        /// <summary>
        /// redis lock time out in milliseconds.
        /// </summary>
        public int? RedisLockTimeout { get; set; }

        /// <summary>
        /// Redis Sentinel model
        /// </summary>
        public bool IsSentinel { get; set; }

        /// <summary>
        /// Redis Sentinel Service Name
        /// </summary>
        public string SentinelServiceName { get; set; }

        /// <summary>
        /// Redis Password
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// Redis DB
        /// </summary>
        public int DbNum { get; set; } = 0;

        /// <summary>
        /// Redis AllowAdmin
        /// </summary>
        public bool AllowAdmin { get; set; }
        #endregion

        #region Implementation of IJobStore

        public Task Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler, CancellationToken cancellationToken = default)
        {
            // a re-entrant call (e.g. a retried scheduler build reusing this instance) would otherwise
            // overwrite _db/_orphanCleanupTimer without disposing the previous ones, leaking a
            // ConnectionMultiplexer and a live Timer per extra call.
            DisposeConnection();

            _storeSchema = new RedisJobStoreSchema(KeyPrefix ?? string.Empty, KeyDelimiter ?? ":");
            if (IsSentinel)
            {
                ConfigurationOptions sentinelOptions = new ConfigurationOptions();
                sentinelOptions.ClientName = Guid.NewGuid().ToString();
                var endPoints = RedisConfiguration.Split(',');
                for (int i = 0; i < endPoints.Length; i++)
                {
                    sentinelOptions.EndPoints.Add(endPoints[i]);
                }
                sentinelOptions.CommandMap = CommandMap.Sentinel;
                sentinelOptions.AllowAdmin = AllowAdmin;
                sentinelOptions.AbortOnConnectFail = false;
                var sentinelConnect = ConnectionMultiplexer.Connect(sentinelOptions);

                ConfigurationOptions redisServiceOptions = new ConfigurationOptions();
                redisServiceOptions.ServiceName = SentinelServiceName;
                redisServiceOptions.Password = Password;
                redisServiceOptions.AbortOnConnectFail = true;
                redisServiceOptions.ClientName = Guid.NewGuid().ToString();
                redisServiceOptions.AllowAdmin = AllowAdmin;
                _db = sentinelConnect.GetSentinelMasterConnection(redisServiceOptions).GetDatabase(DbNum);
            }
            else
            {
                _db = ConnectionMultiplexer.Connect(RedisConfiguration).GetDatabase(DbNum);
            }

            _storage = new RedisStorage(_storeSchema, _db, signaler, InstanceId, TriggerLockTimeout ?? 300000, RedisLockTimeout ?? 6000);

            // sweep for orphaned trigger locks on a fixed cadence, independent of AcquireNextTriggers/the main
            // store lock, so recovery cannot be starved by unrelated store traffic under load.
            var cleanupInterval = TimeSpan.FromMilliseconds((TriggerLockTimeout ?? 300000) / 2.0);
            _orphanCleanupTimer = new Timer(async _ =>
            {
                try
                {
                    await _storage.ReleaseTriggers().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.Error("error while sweeping for orphaned trigger locks", ex);
                }
            }, null, cleanupInterval, cleanupInterval);

            return Task.CompletedTask;
        }

        public Task SchedulerStarted(CancellationToken cancellationToken = default)
        {
            _logger.Debug("scheduler has started");
            return Task.CompletedTask;
        }

        public Task SchedulerPaused(CancellationToken cancellationToken = default)
        {
            _logger.Debug("scheduler has paused");
            return Task.CompletedTask;
        }

        public Task SchedulerResumed(CancellationToken cancellationToken = default)
        {
            _logger.Debug("scheduler has resumed");
            return Task.CompletedTask;
        }

        public Task Shutdown(CancellationToken cancellationToken = default)
        {
            _logger.Debug("scheduler has shutdown");
            DisposeConnection();
            return Task.CompletedTask;
        }

        /// <summary>
        /// disposes the current connection/timer (if any), waiting for any in-flight orphan-cleanup
        /// callback to finish first so it can't be caught mid-flight issuing Redis calls against a
        /// multiplexer this method is about to dispose out from under it.
        /// </summary>
        private void DisposeConnection()
        {
            if (_orphanCleanupTimer != null)
            {
                using (var timerDisposed = new ManualResetEvent(false))
                {
                    _orphanCleanupTimer.Dispose(timerDisposed);
                    timerDisposed.WaitOne();
                }
                _orphanCleanupTimer = null!;
            }

            if (_db != null)
            {
                _db.Multiplexer.Dispose();
                _db = null!;
            }
        }

        public Task StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger, CancellationToken cancellationToken = default)
        {
            _logger.Debug("StoreJobAndTrigger");
            return DoWithLockAsync(async () =>
            {
                await _storage.StoreJob(newJob, false).ConfigureAwait(false);
                await _storage.StoreTrigger(newTrigger, false).ConfigureAwait(false);
            }, () => "Could store job/trigger");
        }

        public Task<bool> IsJobGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            _logger.Debug("IsJobGroupPaused");
            return WithoutLockAsync(() => _storage.IsJobGroupPaused(groupName),
                              () => string.Format("Error on IsJobGroupPaused - Group {0}", groupName));
        }

        public Task<bool> IsTriggerGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            _logger.Debug("IsTriggerGroupPaused");
            return WithoutLockAsync(() => _storage.IsTriggerGroupPaused(groupName),
                              () => string.Format("Error on IsTriggerGroupPaused - Group {0}", groupName));
        }

        public Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            _logger.Debug("StoreJob");
            return DoWithLockAsync(() => _storage.StoreJob(newJob, replaceExisting), () => "Could not store job");
        }

        public Task StoreJobsAndTriggers(IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs, bool replace, CancellationToken cancellationToken = default)
        {
            _logger.Debug("StoreJobsAndTriggers");
            // one lock acquisition for the whole batch instead of one per job - avoids k separate
            // LockTake/LockRelease round trips and k independent contended-wait windows for a batch of k jobs.
            return DoWithLockAsync(async () =>
            {
                foreach (var job in triggersAndJobs)
                {
                    await _storage.StoreJob(job.Key, replace).ConfigureAwait(false);
                    foreach (var trigger in job.Value)
                    {
                        await _storage.StoreTrigger(trigger, replace).ConfigureAwait(false);
                    }
                }
            }, () => "Could store job/trigger");
        }

        public Task<bool> RemoveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RemoveJob");
            return DoWithLockAsync(() => _storage.RemoveJob(jobKey), () => "Could not remove a job");
        }

        public Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RemoveJobs");
            // one lock acquisition for the whole batch instead of one per job.
            return DoWithLockAsync(async () =>
            {
                bool removed = jobKeys.Count > 0;
                foreach (var jobKey in jobKeys)
                {
                    removed = await _storage.RemoveJob(jobKey).ConfigureAwait(false);
                }
                return removed;
            }, () => "Error on removing job");
        }

        public Task<IJobDetail?> RetrieveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RetrieveJob");
            return DoWithLockAsync(() => _storage.RetrieveJob(jobKey),
                              () => "Could not retriev job");
        }

        public Task StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            _logger.Debug("StoreTrigger");
            return DoWithLockAsync(() => _storage.StoreTrigger(newTrigger, replaceExisting),
                             () => "Could not store trigger");
        }

        public Task<bool> RemoveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RemoveTrigger");
            return DoWithLockAsync(() => _storage.RemoveTrigger(triggerKey), () => "Could not remove trigger");
        }

        public Task<bool> RemoveTriggers(IReadOnlyCollection<TriggerKey> triggerKeys, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RemoveTriggers");
            // one lock acquisition for the whole batch instead of one per trigger.
            return DoWithLockAsync(async () =>
            {
                bool removed = triggerKeys.Count > 0;
                foreach (var triggerKey in triggerKeys)
                {
                    removed = await _storage.RemoveTrigger(triggerKey).ConfigureAwait(false);
                }
                return removed;
            }, () => "Error on removing trigger");
        }

        public Task<bool> ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger, CancellationToken cancellationToken = default)
        {
            _logger.Debug("ReplaceTrigger");
            return DoWithLockAsync(() => _storage.ReplaceTrigger(triggerKey, newTrigger), () => "Error on replacing trigger");
        }

        public Task<IOperableTrigger?> RetrieveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RetrieveTrigger");
            return DoWithLockAsync(() => _storage.RetrieveTrigger(triggerKey),
                              () => "could not retrieve trigger");
        }

        public Task<bool> CalendarExists(string calName, CancellationToken cancellationToken = default)
        {
            _logger.Debug("CalendarExists");

            return WithoutLockAsync(() => _storage.CheckExists(calName),
                             () => string.Format("could not check if the calendar {0} exists", calName));
        }

        public Task<bool> CheckExists(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("CheckExists - Job");
            return WithoutLockAsync(() => _storage.CheckExists(jobKey),
                              () => string.Format("could not check if the job {0} exists", jobKey));
        }

        public Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("CheckExists - Trigger");
            return WithoutLockAsync(() => _storage.CheckExists(triggerKey),
                            () => string.Format("could not check if the trigger {0} exists", triggerKey));
        }

        public Task ClearAllSchedulingData(CancellationToken cancellationToken = default)
        {
            _logger.Debug("ClearAllSchedulingData");
            return DoWithLockAsync(() => _storage.ClearAllSchedulingData(), () => "Could not clear all the scheduling data");
        }

        public Task StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers, CancellationToken cancellationToken = default)
        {
            _logger.Debug("StoreCalendar");
            return DoWithLockAsync(() => _storage.StoreCalendar(name, calendar, replaceExisting, updateTriggers),
                       () => string.Format("Error on store calendar - {0}", name));
        }

        public Task<bool> RemoveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RemoveCalendar");
            return DoWithLockAsync(() => _storage.RemoveCalendar(calName),
                       () => string.Format("Error on remvoing calendar - {0}", calName));
        }

        public Task<ICalendar?> RetrieveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            _logger.Debug("RetrieveCalendar");
            return WithoutLockAsync(() => _storage.RetrieveCalendar(calName),
                () => $"Error on retrieving calendar - {calName}");
        }

        public Task<int> GetNumberOfJobs(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetNumberOfJobs");
            return WithoutLockAsync(() => _storage.NumberOfJobs(), () => "Error on getting Number of jobs");
        }

        public Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetNumberOfTriggers");
            return WithoutLockAsync(() => _storage.NumberOfTriggers(), () => "Error on getting number of triggers");
        }

        public Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetNumberOfCalendars");
            return WithoutLockAsync(() => _storage.NumberOfCalendars(), () => "Error on getting number of calendars");
        }

        public Task<IReadOnlyCollection<JobKey>> GetJobKeys(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetJobKeys");
            return WithoutLockAsync(() => _storage.JobKeys(matcher), () => "Error on getting job keys");
        }

        public Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetTriggerKeys");
            return WithoutLockAsync(() => _storage.TriggerKeys(matcher), () => "Error on getting trigger keys");
        }

        public Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetJobGroupNames");
            return WithoutLockAsync(() => _storage.JobGroupNames(), () => "Error on getting job group names");
        }

        public Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetTriggerGroupNames");
            return WithoutLockAsync(() => _storage.TriggerGroupNames(), () => "Error on getting trigger group names");
        }

        public Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetCalendarNames");
            return WithoutLockAsync(() => _storage.CalendarNames(), () => "Error on getting calendar names");
        }

        public Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetTriggersForJob");
            return DoWithLockAsync(() => _storage.GetTriggersForJob(jobKey), () => string.Format("Error on getting triggers for job - {0}", jobKey));
        }

        public Task<TriggerState> GetTriggerState(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetTriggerState");
            return DoWithLockAsync(() => _storage.GetTriggerState(triggerKey),
                () => $"Error on getting trigger state for trigger - {triggerKey}");
        }

        public async Task ResetTriggerFromErrorState(TriggerKey triggerKey, CancellationToken cancellationToken = new CancellationToken())
        {
            var triggerState = await GetTriggerState(triggerKey, cancellationToken).ConfigureAwait(false);

            if (triggerState == TriggerState.Error)
            {
                await DoWithLockAsync(() => _storage.UnsetTriggerState(triggerKey), () => $"Error on unsetting trigger for trigger - {triggerKey}").ConfigureAwait(false);
            }
        }

        public Task PauseTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("PauseTrigger");
            return DoWithLockAsync(() => _storage.PauseTrigger(triggerKey),
                              () => string.Format("Error on pausing trigger - {0}", triggerKey));
        }

        public Task<IReadOnlyCollection<string>> PauseTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            _logger.Debug("PauseTriggers");
            return DoWithLockAsync(() => _storage.PauseTriggers(matcher), () => "Error on pausing triggers");
        }

        public Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("PauseJob");
            return DoWithLockAsync(() => _storage.PauseJob(jobKey), () => string.Format("Error on pausing job - {0}", jobKey));
        }

        public Task<IReadOnlyCollection<string>> PauseJobs(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            _logger.Debug("PauseJobs");
            return DoWithLockAsync(() => _storage.PauseJobs(matcher), () => "Error on pausing jobs");
        }

        public Task ResumeTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("ResumeTrigger");
            return DoWithLockAsync(() => _storage.ResumeTrigger(triggerKey),
                       () => string.Format("Error on resuming trigger - {0}", triggerKey));
        }

        public Task<IReadOnlyCollection<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            _logger.Debug("ResumeTriggers");
            return DoWithLockAsync(() => _storage.ResumeTriggers(matcher), () => "Error on resume triggers");
        }

        public Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(CancellationToken cancellationToken = default)
        {
            _logger.Debug("GetPausedTriggerGroups");
            return WithoutLockAsync(() => _storage.GetPausedTriggerGroups(), () => "Error on getting paused trigger groups");
        }

        public Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            _logger.Debug("ResumeJob");
            return DoWithLockAsync(() => _storage.ResumeJob(jobKey), () => string.Format("Error on resuming job - {0}", jobKey));
        }

        public Task<IReadOnlyCollection<string>> ResumeJobs(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            _logger.Debug("ResumeJobs");
            return DoWithLockAsync(() => _storage.ResumeJobs(matcher), () => "Error on resuming jobs");
        }

        public Task PauseAll(CancellationToken cancellationToken = default)
        {
            _logger.Debug("PauseAll");
            return DoWithLockAsync(() => _storage.PauseAllTriggers(), () => "Error on pausing all");
        }

        public Task ResumeAll(CancellationToken cancellationToken = default)
        {
            _logger.Debug("ResumeAll");
            return DoWithLockAsync(() => _storage.ResumeAllTriggers(), () => "Error on resuming all");
        }

        public Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow, CancellationToken cancellationToken = default)
        {
            _logger.Debug("AcquireNextTriggers");
            // shares the single store lock (via the non-blocking async wait) with every other write/mutating
            // operation - AcquireNextTriggers/ReleaseAcquiredTrigger/TriggersFired/TriggeredJobComplete/etc. all
            // mutate the same trigger-state sorted sets, so they must serialize against each other or a
            // ConcurrentExecutionDisallowed job's triggers can be acquired/fired concurrently (see git history).
            return DoWithLockAsync(() => _storage.AcquireNextTriggers(noLaterThan, maxCount, timeWindow),
                              () => "Error on acquiring next triggers");
        }

        public Task ReleaseAcquiredTrigger(IOperableTrigger trigger, CancellationToken cancellationToken = default)
        {
            _logger.Debug("ReleaseAcquiredTrigger");
            return DoWithLockAsync(() => _storage.ReleaseAcquiredTrigger(trigger),
                () => string.Format("Error on releasing acquired trigger - {0}", trigger));
        }

        /// <summary>
        /// lock this next_fire_time
        /// </summary>
        /// <param name="triggers"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(IReadOnlyCollection<IOperableTrigger> triggers, CancellationToken cancellationToken = default)
        {
            _logger.Debug("TriggersFired");
            return DoWithLockAsync(() => _storage.TriggersFired(triggers), () => "Error on Triggers Fired");
        }

        public Task TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode, CancellationToken cancellationToken = default)
        {
            _logger.Debug("TriggeredJobComplete");
            return DoWithLockAsync(() => _storage.TriggeredJobComplete(trigger, jobDetail, triggerInstCode),
                       () => string.Format("Error on triggered job complete - job:{0} - trigger:{1}", jobDetail, trigger));
        }
        #endregion


        #region private methods

        /// <summary>
        /// runs a pure read against redis with the same error-handling semantics as <see cref="DoWithLockAsync{T}"/>,
        /// but without taking the store lock - use only for genuinely single-key/single-round-trip Redis reads,
        /// which are atomic and don't need app-level mutual exclusion. Composite, multi-round-trip reads
        /// (e.g. reading a trigger's several backing hashes) must go through <see cref="DoWithLockAsync{T}"/>
        /// instead, or they can observe a torn snapshot mid-write.
        /// </summary>
        /// <typeparam name="T">return type of the Function</typeparam>
        /// <param name="fun">Fuction</param>
        /// <param name="errorMessage">lazily-evaluated error message, built only if an exception is actually thrown</param>
        /// <returns></returns>
        private async Task<T> WithoutLockAsync<T>(Func<Task<T>> fun, Func<string>? errorMessage = null)
        {
            try
            {
                return await fun().ConfigureAwait(false);
            }
            catch (ObjectAlreadyExistsException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(errorMessage != null ? errorMessage() : "Job Storage error", ex);
            }
        }

        /// <summary>
        /// crud operation to redis with the store lock held, waited on via <see cref="BaseJobStorage.LockWithWaitAsync"/>
        /// so a contended lock doesn't block a thread-pool thread for the whole wait period. The underlying
        /// storage call itself is awaited (rather than invoked synchronously) so that once the lock is won,
        /// this doesn't hand off to a thread that then blocks on a network round trip either.
        /// </summary>
        /// <typeparam name="T">return type of the Function</typeparam>
        /// <param name="fun">Fuction</param>
        /// <param name="errorMessage">lazily-evaluated error message, built only if an exception is actually thrown</param>
        /// <returns></returns>
        private async Task<T> DoWithLockAsync<T>(Func<Task<T>> fun, Func<string>? errorMessage = null)
        {
            string? lockValue = null;
            try
            {
                lockValue = await _storage.LockWithWaitAsync(_storeSchema.LockKey).ConfigureAwait(false);
                return await fun().ConfigureAwait(false);
            }
            catch (ObjectAlreadyExistsException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(errorMessage != null ? errorMessage() : "Job Storage error", ex);
            }
            finally
            {
                if (lockValue != null)
                {
                    await _storage.UnlockAsync(_storeSchema.LockKey, lockValue).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// <see cref="DoWithLockAsync{T}"/> for actions with no return value. Delegates to the same
        /// implementation so both share identical exception semantics - in particular, both correctly
        /// rethrow <see cref="ObjectAlreadyExistsException"/> instead of silently swallowing it.
        /// </summary>
        /// <param name="action">the async action to run under the lock</param>
        /// <param name="errorMessage">lazily-evaluated error message, built only if an exception is actually thrown</param>
        private Task DoWithLockAsync(Func<Task> action, Func<string>? errorMessage = null)
        {
            return DoWithLockAsync(async () =>
            {
                await action().ConfigureAwait(false);
                return true;
            }, errorMessage);
        }

        #endregion
    }
}
