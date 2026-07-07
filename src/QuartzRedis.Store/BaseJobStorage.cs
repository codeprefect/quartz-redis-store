using System.Collections;
using Common.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Quartz;
using Quartz.Impl.Matchers;
using Quartz.Impl.Triggers;
using Quartz.Spi;
using StackExchange.Redis;

namespace QuartzRedis.Store
{

    /// <summary>
    /// base job storage which could be used by master/slave redis or clustered redis.
    /// </summary>
    public abstract class BaseJobStorage
    {
        /// <summary>
        /// Logger
        /// </summary>
        private readonly ILog _logger;

        /// <summary>
        /// Utc datetime of Epoch.
        /// </summary>
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// RedisJobStoreSchema
        /// </summary>
        protected readonly RedisJobStoreSchema RedisJobStoreSchema;

        /// <summary>
        /// ISchedulerSignaler
        /// </summary>
        protected readonly ISchedulerSignaler SchedulerSignaler;

        /// <summary>
        /// threshold for the misfire (measured in milliseconds)
        /// </summary>
        protected int MisfireThreshold = 60000;

        /// <summary>
        /// redis db. All I/O against it is issued via the async (IDatabaseAsync) API so that lock-critical
        /// paths (waited on via LockWithWaitAsync's non-blocking Task.Delay retry) never hand off to a
        /// thread-pool thread that then blocks synchronously on a network round trip.
        /// </summary>
        protected IDatabase Db;

        /// <summary>
        /// Triggerlock time out here we need to make sure the longest job should not exceed this amount of time, otherwise we need to increase it.
        /// </summary>
        protected int TriggerLockTimeout;

        /// <summary>
        /// redis lock time out in milliseconds.
        /// </summary>
        protected int RedisLockTimeout;

        /// <summary>
        /// scheduler instance id
        /// </summary>
        protected readonly string SchedulerInstanceId;

        /// <summary>
        /// JsonSerializerSettings
        /// </summary>
        private readonly JsonSerializerSettings _serializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All, DateTimeZoneHandling = DateTimeZoneHandling.Utc, NullValueHandling = NullValueHandling.Ignore, ContractResolver = new CamelCasePropertyNamesContractResolver() };

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="redisJobStoreSchema">RedisJobStoreSchema</param>
        /// <param name="db">IDatabase</param>
        /// <param name="signaler">ISchedulerSignaler</param>
        /// <param name="schedulerInstanceId">schedulerInstanceId</param>
        /// <param name="triggerLockTimeout">Trigger lock timeout(number in miliseconds) used in releasing the orphan triggers.</param>
        /// <param name="redisLockTimeout">Redis Lock timeout (number in miliseconds)</param>
        protected BaseJobStorage(RedisJobStoreSchema redisJobStoreSchema, IDatabase db, ISchedulerSignaler signaler, string schedulerInstanceId, int triggerLockTimeout, int redisLockTimeout)
        {
            RedisJobStoreSchema = redisJobStoreSchema;
            Db = db;
            SchedulerSignaler = signaler;
            SchedulerInstanceId = schedulerInstanceId;
            _logger = LogManager.GetLogger(GetType());
            TriggerLockTimeout = triggerLockTimeout;
            RedisLockTimeout = redisLockTimeout;
        }


        /// <summary>
        /// Store the given <see cref="T:Quartz.IJobDetail"/>.
        /// </summary>
        /// <param name="jobDetail">The <see cref="T:Quartz.IJobDetail"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.IJob"/> existing in the
        ///             <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should be
        ///             over-written.
        ///             </param>
        public abstract Task StoreJob(IJobDetail jobDetail, Boolean replaceExisting);

        /// <summary>
        /// Retrieve the <see cref="T:Quartz.IJobDetail"/> for the given
        ///             <see cref="T:Quartz.IJob"/>.
        /// </summary>
        /// <returns>
        /// The desired <see cref="T:Quartz.IJob"/>, or null if there is no match.
        /// </returns>
        public async Task<IJobDetail?> RetrieveJob(JobKey jobKey)
        {
            var jobHashKey = RedisJobStoreSchema.JobHashKey(jobKey);

            var jobDetails = await Db.HashGetAllAsync(jobHashKey).ConfigureAwait(false);

            if (!jobDetails.Any())
            {
                return null;
            }

            var jobDataMapHashKey = RedisJobStoreSchema.JobDataMapHashKey(jobKey);

            HashEntry[] jobDataMap = await Db.HashGetAllAsync(jobDataMapHashKey).ConfigureAwait(false);

            var jobProperties = ConvertToDictionaryString(jobDetails);

            var jobBuilder =
                JobBuilder.Create(Type.GetType(jobProperties[RedisJobStoreSchema.JobClass]))
                          .WithIdentity(jobKey)
                          .WithDescription(jobProperties[RedisJobStoreSchema.Description])
                          .RequestRecovery(Convert.ToBoolean(Convert.ToInt16(jobProperties[RedisJobStoreSchema.RequestRecovery])))
                          .StoreDurably(Convert.ToBoolean(Convert.ToInt16(jobProperties[RedisJobStoreSchema.IsDurable])));


            if (jobDataMap.Any())
            {
                var dataMap = new JobDataMap(ConvertToDictionaryString(jobDataMap) as IDictionary);
                jobBuilder.SetJobData(dataMap);
            }
            return jobBuilder.Build();
        }

        /// <summary>
        /// Store the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <param name="trigger">The <see cref="T:Quartz.ITrigger"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.ITrigger"/> existing in
        ///             the <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should
        ///             be over-written.</param><throws>ObjectAlreadyExistsException </throws>
        public abstract Task StoreTrigger(ITrigger trigger, bool replaceExisting);

        /// <summary>
        /// remove the trigger from all the possible state in the its respective sorted set.
        /// </summary>
        /// <param name="triggerKey">trigger key</param>
        /// <returns>succeeds or not</returns>
        public abstract Task<bool> UnsetTriggerState(TriggerKey triggerKey);

        /// <summary>
        /// remove the trigger state from all the possible sorted set.
        /// </summary>
        /// <param name="triggerHashKey">TriggerHashKey</param>
        /// <returns>succeeds or not</returns>
        public abstract Task<bool> UnsetTriggerState(string triggerHashKey);


        /// <summary>
        /// Store the given <see cref="T:Quartz.ICalendar"/>.
        /// </summary>
        /// <param name="name">The name.</param><param name="calendar">The <see cref="T:Quartz.ICalendar"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.ICalendar"/> existing
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group
        ///             should be over-written.</param><param name="updateTriggers">If <see langword="true"/>, any <see cref="T:Quartz.ITrigger"/>s existing
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/> that reference an existing
        ///             Calendar with the same name with have their next fire time
        ///             re-computed with the new <see cref="T:Quartz.ICalendar"/>.</param><throws>ObjectAlreadyExistsException </throws>
        public abstract Task StoreCalendar(String name, ICalendar calendar, bool replaceExisting, bool updateTriggers);

        /// <summary>
        /// Remove (delete) the <see cref="T:Quartz.ICalendar"/> with the
        ///             given name.
        /// </summary>
        /// <remarks>
        /// If removal of the <see cref="T:Quartz.ICalendar"/> would result in
        ///             <see cref="T:Quartz.ITrigger"/>s pointing to non-existent calendars, then a
        ///             <see cref="T:Quartz.JobPersistenceException"/> will be thrown.
        /// </remarks>
        /// <param name="calendarName">The name of the <see cref="T:Quartz.ICalendar"/> to be removed.</param>
        /// <returns>
        /// <see langword="true"/> if a <see cref="T:Quartz.ICalendar"/> with the given name
        ///             was found and removed from the store.
        /// </returns>
        public abstract Task<bool> RemoveCalendar(String calendarName);

        /// <summary>
        /// Remove (delete) the <see cref="T:Quartz.IJob"/> with the given
        ///             key, and any <see cref="T:Quartz.ITrigger"/> s that reference
        ///             it.
        /// </summary>
        /// <remarks>
        /// If removal of the <see cref="T:Quartz.IJob"/> results in an empty group, the
        ///             group should be removed from the <see cref="T:Quartz.Spi.IJobStore"/>'s list of
        ///             known group names.
        /// </remarks>
        /// <returns>
        /// <see langword="true"/> if a <see cref="T:Quartz.IJob"/> with the given name and
        ///             group was found and removed from the store.
        /// </returns>
        public abstract Task<bool> RemoveJob(JobKey jobKey);

        /// <summary>
        /// Pause all of the <see cref="T:Quartz.IJob"/>s in the given
        ///             group - by pausing all of their <see cref="T:Quartz.ITrigger"/>s.
        /// <para>
        /// The JobStore should "remember" that the group is paused, and impose the
        ///             pause on any new jobs that are added to the group while the group is
        ///             paused.
        /// </para>
        /// </summary>
        /// <seealso cref="T:System.String"/>
        public abstract Task<IReadOnlyCollection<string>> PauseJobs(GroupMatcher<JobKey> matcher);

        /// <summary>
        /// Resume (un-pause) the <see cref="T:Quartz.IJob"/> with the
        ///             given key.
        /// <para>
        /// If any of the <see cref="T:Quartz.IJob"/>'s<see cref="T:Quartz.ITrigger"/> s missed one
        ///             or more fire-times, then the <see cref="T:Quartz.ITrigger"/>'s misfire
        ///             instruction will be applied.
        /// </para>
        /// </summary>
        public async Task ResumeJob(JobKey jobKey)
        {
            foreach (var trigger in await GetTriggersForJob(jobKey).ConfigureAwait(false))
            {
                await ResumeTrigger(trigger.Key).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Pause the <see cref="T:Quartz.IJob"/> with the given key - by
        ///             pausing all of its current <see cref="T:Quartz.ITrigger"/>s.
        /// </summary>
        public async Task PauseJob(JobKey jobKey)
        {
            foreach (var trigger in await GetTriggersForJob(jobKey).ConfigureAwait(false))
            {
                await PauseTrigger(trigger.Key).ConfigureAwait(false);
            }
        }


        /// <summary>
        /// Resume (un-pause) all of the <see cref="T:Quartz.IJob"/>s in
        ///             the given group.
        /// <para>
        /// If any of the <see cref="T:Quartz.IJob"/> s had <see cref="T:Quartz.ITrigger"/> s that
        ///             missed one or more fire-times, then the <see cref="T:Quartz.ITrigger"/>'s
        ///             misfire instruction will be applied.
        /// </para>
        /// </summary>
        public abstract Task<IReadOnlyCollection<string>> ResumeJobs(GroupMatcher<JobKey> matcher);


        /// <summary>
        /// Resume (un-pause) the <see cref="T:Quartz.ITrigger"/> with the
        ///             given key.
        /// <para>
        /// If the <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="T:System.String"/>
        public abstract Task ResumeTrigger(TriggerKey triggerKey);

        /// <summary>
        /// Remove (delete) the <see cref="T:Quartz.ITrigger"/> with the given key.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If removal of the <see cref="T:Quartz.ITrigger"/> results in an empty group, the
        ///             group should be removed from the <see cref="T:Quartz.Spi.IJobStore"/>'s list of
        ///             known group names.
        /// </para>
        /// <para>
        /// If removal of the <see cref="T:Quartz.ITrigger"/> results in an 'orphaned' <see cref="T:Quartz.IJob"/>
        ///             that is not 'durable', then the <see cref="T:Quartz.IJob"/> should be deleted
        ///             also.
        /// </para>
        /// </remarks>
        /// <returns>
        /// <see langword="true"/> if a <see cref="T:Quartz.ITrigger"/> with the given
        ///             name and group was found and removed from the store.
        /// </returns>
        public abstract Task<bool> RemoveTrigger(TriggerKey triggerKey, bool removeNonDurableJob = true);

        /// <summary>
        /// Resume (un-pause) all of the <see cref="T:Quartz.ITrigger"/>s
        ///             in the given group.
        /// <para>
        /// If any <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        public abstract Task<IReadOnlyCollection<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher);

        /// <summary>
        /// Pause the <see cref="T:Quartz.ITrigger"/> with the given key.
        /// </summary>
        public abstract Task PauseTrigger(TriggerKey triggerKey);


        /// <summary>
        /// Pause all triggers - equivalent of calling <see cref="M:Quartz.Spi.IJobStore.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher{Quartz.TriggerKey})"/>
        ///             on every group.
        /// <para>
        /// When <see cref="M:Quartz.Spi.IJobStore.ResumeAll"/> is called (to un-pause), trigger misfire
        ///             instructions WILL be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="M:Quartz.Spi.IJobStore.ResumeAll"/>
        public async Task PauseAllTriggers()
        {
            RedisValue[] triggerGroups = await Db.SetMembersAsync(RedisJobStoreSchema.TriggerGroupsSetKey()).ConfigureAwait(false);
            foreach (var group in triggerGroups)
            {
                await PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(RedisJobStoreSchema.TriggerGroup(group))).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Resume (un-pause) all triggers - equivalent of calling <see cref="M:Quartz.Spi.IJobStore.ResumeTriggers(Quartz.Impl.Matchers.GroupMatcher{Quartz.TriggerKey})"/>
        ///             on every group.
        /// <para>
        /// If any <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="M:Quartz.Spi.IJobStore.PauseAll"/>
        public async Task ResumeAllTriggers()
        {
            RedisValue[] triggerGroups = await Db.SetMembersAsync(RedisJobStoreSchema.TriggerGroupsSetKey()).ConfigureAwait(false);
            foreach (var group in triggerGroups)
            {
                await ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(RedisJobStoreSchema.TriggerGroup(group))).ConfigureAwait(false);
            }
        }

        /// <summary>
        ///  Release triggers from the given current state to the new state if its locking scheduler has not registered as alive in the last triggerlocktimeout
        /// </summary>
        /// <param name="currentState"></param>
        /// <param name="newState"></param>
        protected async Task ReleaseOrphanedTriggers(RedisTriggerState currentState, RedisTriggerState newState)
        {
            SortedSetEntry[] triggers = await Db.SortedSetRangeByScoreWithScoresAsync(RedisJobStoreSchema.TriggerStateSetKey(currentState), double.NegativeInfinity, double.PositiveInfinity).ConfigureAwait(false);

            // Blocked/PausedBlocked triggers are never themselves locked (LockTrigger is only ever called
            // on the trigger that AcquireNextTriggers is acquiring for firing) - they're stuck only for as
            // long as the job that blocked them is still actively being executed by a live scheduler.
            bool checkBlockingJobInsteadOfOwnLock = currentState == RedisTriggerState.Blocked || currentState == RedisTriggerState.PausedBlocked;

            foreach (var sortedSetEntry in triggers)
            {
                string triggerHashKey = sortedSetEntry.Element.ToString();

                bool orphaned = checkBlockingJobInsteadOfOwnLock
                    ? !await IsBlockingJobStillActive(triggerHashKey).ConfigureAwait(false)
                    : string.IsNullOrEmpty(await Db.StringGetAsync(RedisJobStoreSchema.TriggerLockKey(RedisJobStoreSchema.TriggerKey(triggerHashKey))).ConfigureAwait(false));

                // Lock key has expired (or the blocking job is no longer active). We can safely alter the trigger's state.
                if (orphaned)
                {
                    await SetTriggerState(newState, sortedSetEntry.Score, sortedSetEntry.Element, currentState).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// determines whether the job that put the given (Blocked/PausedBlocked) trigger into its current state
        /// is still actively held by a live scheduler - i.e. whether any of the job's sibling triggers is
        /// currently Acquired and still holds its per-trigger lock.
        /// </summary>
        /// <param name="triggerHashKey">hash key of the Blocked/PausedBlocked trigger</param>
        /// <returns>true if the blocking job still appears to be executing</returns>
        private async Task<bool> IsBlockingJobStillActive(string triggerHashKey)
        {
            var jobHashKey = await Db.HashGetAsync(triggerHashKey, RedisJobStoreSchema.JobHash).ConfigureAwait(false);
            if (string.IsNullOrEmpty(jobHashKey))
            {
                return false;
            }

            var jobTriggersSetKey = RedisJobStoreSchema.JobTriggersSetKey(RedisJobStoreSchema.JobKey(jobHashKey));

            foreach (var siblingHashKey in await Db.SetMembersAsync(jobTriggersSetKey).ConfigureAwait(false))
            {
                var siblingTriggerKey = RedisJobStoreSchema.TriggerKey(siblingHashKey.ToString());
                if (!string.IsNullOrEmpty(await Db.StringGetAsync(RedisJobStoreSchema.TriggerLockKey(siblingTriggerKey)).ConfigureAwait(false)))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Remove (delete) the <see cref="T:Quartz.ITrigger"/> with the
        ///             given name, and store the new given one - which must be associated
        ///             with the same job.
        /// </summary>
        /// <param name="triggerKey">The <see cref="T:Quartz.ITrigger"/> to be replaced.</param><param name="newTrigger">The new <see cref="T:Quartz.ITrigger"/> to be stored.</param>
        /// <returns>
        /// <see langword="true"/> if a <see cref="T:Quartz.ITrigger"/> with the given
        ///             name and group was found and removed from the store.
        /// </returns>
        public async Task<bool> ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            var oldTrigger = await RetrieveTrigger(triggerKey).ConfigureAwait(false);

            bool found = oldTrigger != null;

            if (found)
            {
                if (!oldTrigger.JobKey.Equals(newTrigger.JobKey))
                {
                    throw new JobPersistenceException("New Trigger is not linked to the same job as the old trigger");
                }

                await RemoveTrigger(triggerKey, false).ConfigureAwait(false);
                await StoreTrigger(newTrigger, false).ConfigureAwait(false);
            }

            return found;
        }

        /// <summary>
        /// Retrieve the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <returns>
        /// The desired <see cref="T:Quartz.ITrigger"/>, or null if there is no
        ///             match.
        /// </returns>
        public async Task<IOperableTrigger> RetrieveTrigger(TriggerKey triggerKey)
        {
            var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(triggerKey);

            var properties = await Db.HashGetAllAsync(triggerHashKey).ConfigureAwait(false);

            if (properties != null && properties.Count() > 0)
            {
                var trigger = RetrieveTrigger(triggerKey, ConvertToDictionaryString(properties));

                if (trigger != null)
                {
                    var dataMapEntries = await Db.HashGetAllAsync(RedisJobStoreSchema.TriggerDataMapHashKey(triggerKey)).ConfigureAwait(false);
                    if (dataMapEntries != null && dataMapEntries.Any())
                    {
                        foreach (var entry in ConvertToDictionaryString(dataMapEntries))
                        {
                            trigger.JobDataMap[entry.Key] = entry.Value;
                        }
                    }
                }

                return trigger;
            }

            _logger.WarnFormat("trigger does not exist - {0}", triggerHashKey);
            return null;

        }

        /// <summary>
        /// Release triggers currently held by schedulers which have ceased to function e.g. crashed.
        /// </summary>
        /// <remarks>
        /// This uses its own dedicated lock (<see cref="RedisJobStoreSchema.OrphanCleanupLockKey"/>), separate
        /// from the main store lock, and takes it non-blockingly. That way orphaned-trigger cleanup can never be
        /// starved by unrelated store traffic contending for the main lock - it either runs on schedule or is
        /// already being run by another concurrent caller/instance, in which case this call is a no-op.
        /// </remarks>
        public async Task ReleaseTriggers()
        {
            // this is only ever invoked on a fixed TriggerLockTimeout/2 cadence (see RedisJobStore's
            // _orphanCleanupTimer) - match that cadence here, otherwise every other tick is a guaranteed
            // no-op (elapsed never exceeds the full TriggerLockTimeout on a tick that's only T/2 later).
            double releaseThreshold = TriggerLockTimeout / 2.0;

            double misfireTime = DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds();
            if (misfireTime - await GetLastTriggersReleaseTime().ConfigureAwait(false) <= releaseThreshold)
            {
                return;
            }

            var cleanupLockValue = Guid.NewGuid().ToString();
            if (!await Db.LockTakeAsync(RedisJobStoreSchema.OrphanCleanupLockKey, cleanupLockValue, TimeSpan.FromMilliseconds(RedisLockTimeout)).ConfigureAwait(false))
            {
                // another caller/instance is already sweeping for orphaned triggers - nothing to do here.
                return;
            }

            try
            {
                // re-check under the lock in case another caller just finished the sweep.
                misfireTime = DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds();
                if (misfireTime - await GetLastTriggersReleaseTime().ConfigureAwait(false) <= releaseThreshold)
                {
                    return;
                }

                // it has been more than releaseThreshold milliseconds since we last released orphaned triggers
                await ReleaseOrphanedTriggers(RedisTriggerState.Acquired, RedisTriggerState.Waiting).ConfigureAwait(false);
                await ReleaseOrphanedTriggers(RedisTriggerState.Blocked, RedisTriggerState.Waiting).ConfigureAwait(false);
                await ReleaseOrphanedTriggers(RedisTriggerState.PausedBlocked, RedisTriggerState.Paused).ConfigureAwait(false);
                await SetLastTriggerReleaseTime(DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds()).ConfigureAwait(false);
            }
            finally
            {
                if (!await Db.LockReleaseAsync(RedisJobStoreSchema.OrphanCleanupLockKey, cleanupLockValue).ConfigureAwait(false))
                {
                    _logger.Warn("orphan cleanup lock was no longer held on release - it likely expired mid-sweep and was taken over by another instance");
                }
            }
        }

        /// <summary>
        /// Get a handle to the next trigger to be fired, and mark it as 'reserved'
        ///             by the calling scheduler.
        /// </summary>
        /// <param name="noLaterThan">If &gt; 0, the JobStore should only return a Trigger
        ///             that will fire no later than the time represented in this value as
        ///             milliseconds.</param><param name="maxCount"/><param name="timeWindow"/>
        /// <returns/>
        /// <seealso cref="T:Quartz.ITrigger"/>
        public async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            // orphaned-trigger recovery now runs entirely off RedisJobStore's independent background timer
            // (see ReleaseTriggers's own remarks) - calling it here would run its unbounded sweep cost inline
            // while the caller is holding the store lock, inflating lock hold time under exactly the load this
            // method needs to stay fast under.

            var triggers = new List<IOperableTrigger>();

            bool retry = false;

            // A burst of misfired triggers would otherwise restart this scan indefinitely while holding the
            // trigger-acquisition lock, blocking every other caller of AcquireNextTriggers/ReleaseAcquiredTrigger
            // for an unbounded time. Cap the retries and let the scheduler simply call again on its next poll.
            const int maxMisfireRetries = 10;
            var attempt = 0;

            // must accumulate across retries (not be recreated per-iteration) - otherwise a misfire on one
            // trigger that restarts the scan forgets which non-concurrent jobs were already claimed earlier
            // in this same call, letting two triggers of the same DisallowConcurrentExecution job both be acquired.
            var acquiredJobHashKeysForNoConcurrentExec = new global::System.Collections.Generic.HashSet<string>();

            do
            {
                retry = false;
                attempt++;

                // each retry re-scans Waiting from scratch, so cap the fetch (and the acquisitions taken
                // from it) to whatever's still needed - otherwise a misfire partway through one batch,
                // followed by a full new batch on retry, can return more than the caller's requested maxCount.
                var remaining = maxCount - triggers.Count;
                if (remaining <= 0)
                {
                    break;
                }

                var score = ToUnixTimeMilliseconds(noLaterThan.Add(timeWindow));

                var waitingStateTriggers =
                    await Db.SortedSetRangeByScoreWithScoresAsync(
                        RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Waiting), 0, score,
                        Exclude.None, Order.Ascending, 0, remaining).ConfigureAwait(false);
                foreach (var sortedSetEntry in waitingStateTriggers)
                {
                    if (triggers.Count >= maxCount)
                    {
                        break;
                    }

                    var trigger = await RetrieveTrigger(RedisJobStoreSchema.TriggerKey(sortedSetEntry.Element)).ConfigureAwait(false);

                    if (trigger == null)
                    {
                        continue;
                    }

                    if (await ApplyMisfire(trigger).ConfigureAwait(false))
                    {
                        retry = true;
                        break;
                    }

                    if (trigger.GetNextFireTimeUtc() == null)
                    {
                        await this.UnsetTriggerState(sortedSetEntry.Element).ConfigureAwait(false);
                        continue;
                    }

                    var jobHashKey = RedisJobStoreSchema.JobHashKey(trigger.JobKey);

                    var job = await RetrieveJob(trigger.JobKey).ConfigureAwait(false);

                    if (job != null && job.ConcurrentExecutionDisallowed)
                    {
                        if (acquiredJobHashKeysForNoConcurrentExec.Contains(jobHashKey))
                        {
                            continue;
                        }
                        acquiredJobHashKeysForNoConcurrentExec.Add(jobHashKey);
                    }

                    await LockTrigger(trigger.Key).ConfigureAwait(false);
                    await SetTriggerState(RedisTriggerState.Acquired,
                                         sortedSetEntry.Score, sortedSetEntry.Element, RedisTriggerState.Waiting).ConfigureAwait(false);
                    triggers.Add(trigger);
                }




            } while (retry && attempt < maxMisfireRetries);


            return triggers;
        }

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> that the scheduler no longer plans to
        ///             fire the given <see cref="T:Quartz.ITrigger"/>, that it had previously acquired
        ///             (reserved).
        /// </summary>
        public async Task ReleaseAcquiredTrigger(IOperableTrigger trigger)
        {
            var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(trigger.Key);

            var score =
                await Db.SortedSetScoreAsync(RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Acquired),
                                        triggerHashKey).ConfigureAwait(false);

            if (score.HasValue)
            {
                if (trigger.GetNextFireTimeUtc().HasValue)
                {
                    await SetTriggerState(RedisTriggerState.Waiting,
                                         trigger.GetNextFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey, RedisTriggerState.Acquired).ConfigureAwait(false);
                }
                else
                {
                    await this.UnsetTriggerState(triggerHashKey).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> that the scheduler is now firing the
        ///             given <see cref="T:Quartz.ITrigger"/> (executing its associated <see cref="T:Quartz.IJob"/>),
        ///             that it had previously acquired (reserved).
        /// </summary>
        /// <returns>
        /// May return null if all the triggers or their calendars no longer exist, or
        ///             if the trigger was not successfully put into the 'executing'
        ///             state.  Preference is to return an empty list if none of the triggers
        ///             could be fired.
        /// </returns>
        public abstract Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(IReadOnlyCollection<IOperableTrigger> triggers);

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> that the scheduler has completed the
        ///             firing of the given <see cref="T:Quartz.ITrigger"/> (and the execution its
        ///             associated <see cref="T:Quartz.IJob"/>), and that the <see cref="T:Quartz.JobDataMap"/>
        ///             in the given <see cref="T:Quartz.IJobDetail"/> should be updated if the <see cref="T:Quartz.IJob"/>
        ///             is stateful.
        /// </summary>
        public abstract Task TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail,
                                                  SchedulerInstruction triggerInstCode);

        /// <summary>
        /// Retrieve the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <param name="calName">The name of the <see cref="T:Quartz.ICalendar"/> to be retrieved.</param>
        /// <returns>
        /// The desired <see cref="T:Quartz.ICalendar"/>, or null if there is no
        ///             match.
        /// </returns>
        public async Task<ICalendar> RetrieveCalendar(string calName)
        {
            var calendarHashKey = RedisJobStoreSchema.CalendarHashKey(calName);
            ICalendar calendar = null;

            HashEntry[] calendarPropertiesInRedis = await Db.HashGetAllAsync(calendarHashKey).ConfigureAwait(false);

            if (calendarPropertiesInRedis != null && calendarPropertiesInRedis.Count() > 0)
            {

                var calendarProperties = ConvertToDictionaryString(calendarPropertiesInRedis);

                calendar =
                    JsonConvert.DeserializeObject(calendarProperties[RedisJobStoreSchema.CalendarSerialized],
                                                  _serializerSettings) as ICalendar;
            }

            return calendar;
        }


        /// <summary>
        /// Get all of the Triggers that are associated to the given Job.
        /// </summary>
        /// <remarks>
        /// If there are no matches, a zero-length array should be returned.
        /// </remarks>
        public async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(JobKey jobKey)
        {
            var jobTriggerSetKey = RedisJobStoreSchema.JobTriggersSetKey(jobKey);
            var triggerHashKeys = await Db.SetMembersAsync(jobTriggerSetKey).ConfigureAwait(false);

            var result = new List<IOperableTrigger>();
            foreach (var triggerHashKey in triggerHashKeys)
            {
                var trigger = await RetrieveTrigger(RedisJobStoreSchema.TriggerKey(triggerHashKey)).ConfigureAwait(false);
                if (trigger != null)
                {
                    result.Add(trigger);
                }
            }
            return result;
        }

        /// <summary>
        /// Gets the paused trigger groups.
        /// </summary>
        /// <returns/>
        public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroups()
        {
            RedisValue[] triggerGroupSetKeys =
                await Db.SetMembersAsync(RedisJobStoreSchema.PausedTriggerGroupsSetKey()).ConfigureAwait(false);

            var groups = new global::System.Collections.Generic.HashSet<string>();

            foreach (var triggerGroupSetKey in triggerGroupSetKeys)
            {
                groups.Add(RedisJobStoreSchema.TriggerGroup(triggerGroupSetKey));
            }

            return groups;

        }

        /// <summary>
        /// returns true if the given JobGroup is paused
        /// </summary>
        /// <param name="groupName"/>
        /// <returns/>
        public Task<bool> IsJobGroupPaused(string groupName)
        {
            return
                Db.SetContainsAsync(RedisJobStoreSchema.PausedJobGroupsSetKey(),
                                     RedisJobStoreSchema.JobGroupSetKey(groupName));
        }

        /// <summary>
        /// returns true if the given TriggerGroup
        ///             is paused
        /// </summary>
        /// <param name="groupName"/>
        /// <returns/>
        public Task<bool> IsTriggerGroupPaused(string groupName)
        {
            return
                Db.SetContainsAsync(RedisJobStoreSchema.PausedTriggerGroupsSetKey(),
                                    RedisJobStoreSchema.TriggerGroupSetKey(groupName));
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.IJob"/>s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public async Task<int> NumberOfJobs()
        {
            return (int)await Db.SetLengthAsync(RedisJobStoreSchema.JobsSetKey()).ConfigureAwait(false);
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.ITrigger"/>s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public async Task<int> NumberOfTriggers()
        {
            return (int)await Db.SetLengthAsync(RedisJobStoreSchema.TriggersSetKey()).ConfigureAwait(false);
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.ICalendar"/> s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public async Task<int> NumberOfCalendars()
        {
            return (int)await Db.SetLengthAsync(RedisJobStoreSchema.CalendarsSetKey()).ConfigureAwait(false);
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.IJob"/> s that
        ///             have the given group name.
        /// <para>
        /// If there are no jobs in the given group name, the result should be a
        ///             zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        /// <param name="matcher"/>
        /// <returns/>
        public abstract Task<System.Collections.Generic.IReadOnlyCollection<JobKey>> JobKeys(GroupMatcher<JobKey> matcher);

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ITrigger"/>s
        ///             that have the given group name.
        /// <para>
        /// If there are no triggers in the given group name, the result should be a
        ///             zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public abstract Task<System.Collections.Generic.IReadOnlyCollection<TriggerKey>> TriggerKeys(GroupMatcher<TriggerKey> matcher);

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.IJob"/>
        ///             groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        ///             array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public async Task<IReadOnlyCollection<string>> JobGroupNames()
        {
            RedisValue[] groupsSet = await Db.SetMembersAsync(RedisJobStoreSchema.JobGroupsSetKey()).ConfigureAwait(false);

            return groupsSet.Select(g => RedisJobStoreSchema.JobGroup(g)).ToList();
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ITrigger"/>
        ///             groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        ///             array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public async Task<IReadOnlyCollection<string>> TriggerGroupNames()
        {
            RedisValue[] groupsSet = await Db.SetMembersAsync(RedisJobStoreSchema.TriggerGroupsSetKey()).ConfigureAwait(false);

            return groupsSet.Select(g => RedisJobStoreSchema.TriggerGroup(g)).ToList();
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ICalendar"/> s
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// <para>
        /// If there are no Calendars in the given group name, the result should be
        ///             a zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public async Task<IReadOnlyCollection<string>> CalendarNames()
        {
            RedisValue[] calendarsSet = await Db.SetMembersAsync(RedisJobStoreSchema.CalendarsSetKey()).ConfigureAwait(false);

            return calendarsSet.Select(g => RedisJobStoreSchema.GetCalendarName(g)).ToList();
        }

        /// <summary>
        /// Get the current state of the identified <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <seealso cref="T:Quartz.TriggerState"/>
        public abstract Task<TriggerState> GetTriggerState(TriggerKey triggerKey);

        /// <summary>
        /// Pause all of the <see cref="T:Quartz.ITrigger"/>s in the
        ///             given group.
        /// </summary>
        /// <remarks>
        /// The JobStore should "remember" that the group is paused, and impose the
        ///             pause on any new triggers that are added to the group while the group is
        ///             paused.
        /// </remarks>
        public abstract Task<IReadOnlyCollection<string>> PauseTriggers(GroupMatcher<TriggerKey> matcher);


        /// <summary>
        ///  Determine whether or not the given trigger has misfired.If so, notify {SchedulerSignaler} and update the trigger.
        /// </summary>
        /// <param name="trigger">IOperableTrigger</param>
        /// <returns>applied or not</returns>
        protected async Task<bool> ApplyMisfire(IOperableTrigger trigger)
        {
            double misfireTime = DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds();
            double score = misfireTime;

            if (MisfireThreshold > 0)
            {
                misfireTime = misfireTime - MisfireThreshold;
            }

            //if the trigger has no next fire time or exceeds the misfirethreshold or enable ignore misfirepolicy
            // then dont apply misfire.
            DateTimeOffset? nextFireTime = trigger.GetNextFireTimeUtc();

            if (nextFireTime.HasValue == false ||
               (nextFireTime.HasValue && nextFireTime.Value.DateTime.ToUnixTimeMilliSeconds() > misfireTime) ||
               trigger.MisfireInstruction == -1)
            {
                return false;
            }

            ICalendar calendar = null;

            if (!string.IsNullOrEmpty(trigger.CalendarName))
            {
                calendar = await RetrieveCalendar(trigger.CalendarName).ConfigureAwait(false);
            }

            SchedulerSignaler.NotifyTriggerListenersMisfired((IOperableTrigger)trigger.Clone());

            trigger.UpdateAfterMisfire(calendar);

            await StoreTrigger(trigger, true).ConfigureAwait(false);

            var updatedNextFireTime = trigger.GetNextFireTimeUtc();

            if (!updatedNextFireTime.HasValue)
            {
                await SetTriggerState(RedisTriggerState.Completed,
                                     score, RedisJobStoreSchema.TriggerHashkey(trigger.Key)).ConfigureAwait(false);
                SchedulerSignaler.NotifySchedulerListenersFinalized(trigger);
            }
            else if (nextFireTime.Equals(updatedNextFireTime))
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Check if the job identified by the given key exists in storage
        /// </summary>
        /// <param name="jobKey">Jobkey</param>
        /// <returns>exists or not</returns>
        public Task<bool> CheckExists(JobKey jobKey)
        {
            return Db.KeyExistsAsync(RedisJobStoreSchema.JobHashKey(jobKey));
        }


        /// <summary>
        /// Check if the calendar identified by the given name exists
        /// </summary>
        /// <param name="calName">Calendar Name</param>
        /// <returns>exists or not</returns>
        public Task<bool> CheckExists(string calName)
        {
            return Db.KeyExistsAsync(RedisJobStoreSchema.CalendarHashKey(calName));
        }


        /// <summary>
        /// Check if the trigger identified by the given key exists
        /// </summary>
        /// <param name="triggerKey">TriggerKey</param>
        /// <returns>exists or not</returns>
        public Task<bool> CheckExists(TriggerKey triggerKey)
        {
            return Db.KeyExistsAsync(RedisJobStoreSchema.TriggerHashkey(triggerKey));
        }

        /// <summary>
        /// delete all scheduling data - all jobs, triggers and calendars. Scheduler.Clear()
        /// </summary>
        public async Task ClearAllSchedulingData()
        {
            // delete triggers
            foreach (string jobHashKey in
                await Db.SetMembersAsync(RedisJobStoreSchema.JobsSetKey()).ConfigureAwait(false))
            {
                await RemoveJob(RedisJobStoreSchema.JobKey(jobHashKey)).ConfigureAwait(false);
            }

            foreach (var triggerHashKey in await Db.SetMembersAsync(RedisJobStoreSchema.TriggersSetKey()).ConfigureAwait(false))
            {
                await RemoveTrigger(RedisJobStoreSchema.TriggerKey(triggerHashKey)).ConfigureAwait(false);
            }

            foreach (var calHashName in await Db.SetMembersAsync(RedisJobStoreSchema.CalendarsSetKey()).ConfigureAwait(false))
            {
                await Db.KeyDeleteAsync(RedisJobStoreSchema.CalendarTriggersSetKey(RedisJobStoreSchema.GetCalendarName(calHashName))).ConfigureAwait(false);
                await RemoveCalendar(RedisJobStoreSchema.GetCalendarName(calHashName)).ConfigureAwait(false);
            }

            await Db.KeyDeleteAsync(RedisJobStoreSchema.PausedTriggerGroupsSetKey()).ConfigureAwait(false);
            await Db.KeyDeleteAsync(RedisJobStoreSchema.PausedJobGroupsSetKey()).ConfigureAwait(false);
        }


        /// <summary>
        /// Retrieve the last time (in milliseconds) that orphaned triggers were released
        /// </summary>
        /// <returns>time in milli seconds from epoch time</returns>
        protected async Task<double> GetLastTriggersReleaseTime()
        {
            var lastReleaseTime = await Db.StringGetAsync(RedisJobStoreSchema.LastTriggerReleaseTime()).ConfigureAwait(false);

            if (string.IsNullOrEmpty(lastReleaseTime))
            {
                return 0;
            }
            return double.Parse(lastReleaseTime);
        }

        /// <summary>
        /// Set the last time at which orphaned triggers were released
        /// </summary>
        /// <param name="time">time in milli seconds from epoch time</param>
        protected Task SetLastTriggerReleaseTime(double time)
        {
            return Db.StringSetAsync(RedisJobStoreSchema.LastTriggerReleaseTime(), time);
        }

        /// <summary>
        /// Set a trigger state by adding the trigger to the relevant sorted set, using its next fire time as the score.
        /// Also keeps the trigger's cached "current state" hash field (<see cref="RedisJobStoreSchema.CurrentState"/>)
        /// in sync, so <see cref="GetTriggerState"/> can read it directly instead of scanning every state's sorted set.
        /// </summary>
        /// <param name="state">RedisTriggerState</param>
        /// <param name="score">time in milli seconds from epoch time</param>
        /// <param name="triggerHashKey">TriggerHashKey</param>
        /// <param name="knownCurrentState">if the caller already knows which single state the trigger is
        /// currently in, pass it here to remove the trigger from just that one sorted set instead of blindly
        /// scanning/removing from every possible state (7 round trips) - a significant win on hot paths like
        /// AcquireNextTriggers where the prior state is always known. Leave null for the safe, general fallback.
        /// Acts as a compare-and-swap: if the trigger is no longer in the presumed prior state (some other
        /// caller already moved it since this caller last observed it - e.g. the independent orphan-cleanup
        /// sweep racing a fresh TriggersFired/ReleaseAcquiredTrigger, since that sweep intentionally runs
        /// outside the main store lock), this is a no-op rather than blindly resurrecting a stale transition.</param>
        /// <returns>succeeds or not</returns>
        protected async Task<bool> SetTriggerState(RedisTriggerState state, double score, string triggerHashKey, RedisTriggerState? knownCurrentState = null)
        {
            // Moving a trigger INTO Acquired must never clear TriggerLockKey - LockTrigger sets that lock
            // immediately before this call (see AcquireNextTriggers), and it needs to survive so
            // ReleaseOrphanedTriggers can tell a live Acquired trigger apart from an orphaned one.
            bool clearLockOnRemoval = state != RedisTriggerState.Acquired;

            if (knownCurrentState.HasValue)
            {
                if (!await Db.SortedSetRemoveAsync(RedisJobStoreSchema.TriggerStateSetKey(knownCurrentState.Value), triggerHashKey).ConfigureAwait(false))
                {
                    return false;
                }
                if (clearLockOnRemoval)
                {
                    await Db.KeyDeleteAsync(RedisJobStoreSchema.TriggerLockKey(RedisJobStoreSchema.TriggerKey(triggerHashKey))).ConfigureAwait(false);
                }
            }
            else if (clearLockOnRemoval)
            {
                await this.UnsetTriggerState(triggerHashKey).ConfigureAwait(false);
            }
            else
            {
                // remove from every other state's sorted set without touching the trigger's lock key.
                foreach (RedisTriggerState otherState in Enum.GetValues(typeof(RedisTriggerState)))
                {
                    if (otherState != RedisTriggerState.Acquired)
                    {
                        await Db.SortedSetRemoveAsync(RedisJobStoreSchema.TriggerStateSetKey(otherState), triggerHashKey).ConfigureAwait(false);
                    }
                }
            }

            await Db.HashSetAsync(triggerHashKey, RedisJobStoreSchema.CurrentState, state.ToString()).ConfigureAwait(false);
            return await Db.SortedSetAddAsync(RedisJobStoreSchema.TriggerStateSetKey(state), triggerHashKey, score).ConfigureAwait(false);
        }

        /// <summary>
        /// convert JobDataMap to hashEntry array
        /// </summary>
        /// <param name="jobDataMap">JobDataMap</param>
        /// <returns>HashEntry[]</returns>
        protected HashEntry[] ConvertToHashEntries(JobDataMap jobDataMap)
        {
            var entries = new List<HashEntry>();
            if (jobDataMap != null)
            {
                entries.AddRange(jobDataMap.Select(entry => new HashEntry(entry.Key, entry.Value?.ToString() ?? "")));
            }
            return entries.ToArray();
        }

        /// <summary>
        /// convert hashEntry array to Dictionary
        /// </summary>
        /// <param name="entries">HashEntry[]</param>
        /// <returns>IDictionary{string, string}</returns>
        protected IDictionary<string, string> ConvertToDictionaryString(HashEntry[] entries)
        {
            var stringMap = new Dictionary<string, string>();
            if (entries != null)
            {
                foreach (var entry in entries)
                {
                    stringMap.Add(entry.Name, entry.Value.ToString());
                }
            }
            return stringMap;
        }


        /// <summary>
        /// convert IJobDetail to HashEntry array
        /// </summary>
        /// <param name="jobDetail">JobDetail</param>
        /// <returns>Array of <see cref="HashEntry"/></returns>
        protected HashEntry[] ConvertToHashEntries(IJobDetail jobDetail)
        {
            var entries = new List<HashEntry>
                {
                    new HashEntry(RedisJobStoreSchema.JobClass, jobDetail.JobType.AssemblyQualifiedName),
                    new HashEntry(RedisJobStoreSchema.Description, jobDetail.Description ?? ""),
                    new HashEntry(RedisJobStoreSchema.IsDurable, jobDetail.Durable),
                    new HashEntry(RedisJobStoreSchema.RequestRecovery,jobDetail.RequestsRecovery),
                    new HashEntry(RedisJobStoreSchema.BlockedBy, ""),
                    new HashEntry(RedisJobStoreSchema.BlockTime, "")
                };

            return entries.ToArray();
        }

        /// <summary>
        /// convert trigger to HashEntry array
        /// </summary>
        /// <param name="trigger">Trigger</param>
        /// <returns>Array of <see cref="HashEntry"/></returns>
        protected HashEntry[] ConvertToHashEntries(ITrigger trigger)
        {
            var operableTrigger = trigger as IOperableTrigger;
            if (operableTrigger == null)
            {
                throw new InvalidCastException("trigger needs to be IOperable");
            }

            var entries = new List<HashEntry>
                {
                    new HashEntry(RedisJobStoreSchema.JobHash, this.RedisJobStoreSchema.JobHashKey(operableTrigger.JobKey)),
                    new HashEntry(RedisJobStoreSchema.Description, operableTrigger.Description ?? ""),
                    new HashEntry(RedisJobStoreSchema.NextFireTime, operableTrigger.GetNextFireTimeUtc().HasValue? operableTrigger.GetNextFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds().ToString():""),
                    new HashEntry(RedisJobStoreSchema.PrevFireTime, operableTrigger.GetPreviousFireTimeUtc().HasValue? operableTrigger.GetPreviousFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds().ToString():""),
                    new HashEntry(RedisJobStoreSchema.Priority, operableTrigger.Priority),
                    new HashEntry(RedisJobStoreSchema.StartTime,operableTrigger.StartTimeUtc.DateTime.ToUnixTimeMilliSeconds().ToString()),
                    new HashEntry(RedisJobStoreSchema.EndTime, operableTrigger.EndTimeUtc.HasValue?operableTrigger.EndTimeUtc.Value.DateTime.ToUnixTimeMilliSeconds().ToString():""),
                    new HashEntry(RedisJobStoreSchema.FinalFireTime,operableTrigger.FinalFireTimeUtc.HasValue?operableTrigger.FinalFireTimeUtc.Value.DateTime.ToUnixTimeMilliSeconds().ToString():""),
                    new HashEntry(RedisJobStoreSchema.FireInstanceId,operableTrigger.FireInstanceId??string.Empty),
                    new HashEntry(RedisJobStoreSchema.MisfireInstruction,operableTrigger.MisfireInstruction),
                    new HashEntry(RedisJobStoreSchema.CalendarName,operableTrigger.CalendarName??string.Empty)
                };

            if (operableTrigger is ISimpleTrigger)
            {
                entries.Add(new HashEntry(RedisJobStoreSchema.TriggerType, RedisJobStoreSchema.TriggerTypeSimple));
                entries.Add(new HashEntry(RedisJobStoreSchema.RepeatCount, ((ISimpleTrigger)operableTrigger).RepeatCount));
                entries.Add(new HashEntry(RedisJobStoreSchema.RepeatInterval, ((ISimpleTrigger)operableTrigger).RepeatInterval.ToString()));
                entries.Add(new HashEntry(RedisJobStoreSchema.TimesTriggered, ((ISimpleTrigger)operableTrigger).TimesTriggered));
            }
            else if (operableTrigger is ICronTrigger)
            {
                entries.Add(new HashEntry(RedisJobStoreSchema.TriggerType, RedisJobStoreSchema.TriggerTypeCron));
                entries.Add(new HashEntry(RedisJobStoreSchema.CronExpression, ((ICronTrigger)operableTrigger).CronExpressionString));
                entries.Add(new HashEntry(RedisJobStoreSchema.TimeZoneId, ((ICronTrigger)operableTrigger).TimeZone.Id));
            }


            return entries.ToArray();
        }

        /// <summary>
        /// convert Calendar to HashEntry array
        /// </summary>
        /// <param name="calendar"></param>
        /// <returns></returns>
        protected HashEntry[] ConvertToHashEntries(ICalendar calendar)
        {
            var entries = new List<HashEntry>
                {
                    new HashEntry(RedisJobStoreSchema.CalendarSerialized, JsonConvert.SerializeObject(calendar,_serializerSettings))
                };

            return entries.ToArray();
        }

        /// <summary>
        /// Lock the trigger with the given key to the current job scheduler
        /// </summary>
        /// <param name="triggerKey">TriggerKey</param>
        /// <returns>succeed or not</returns>
        protected Task<bool> LockTrigger(TriggerKey triggerKey)
        {
            return Db.StringSetAsync(RedisJobStoreSchema.TriggerLockKey(triggerKey), SchedulerInstanceId, TimeSpan.FromMilliseconds(TriggerLockTimeout));
        }


        #region private methods

        /// <summary>
        /// convert trigger properties into different IOperableTrigger object (simple or cron trigger)
        /// </summary>
        /// <param name="triggerKey">TriggerKey</param>
        /// <param name="properties">trigger's properties</param>
        /// <returns>IOperableTrigger</returns>
        private IOperableTrigger RetrieveTrigger(TriggerKey triggerKey, IDictionary<string, string> properties)
        {
            var type = properties[RedisJobStoreSchema.TriggerType];

            if (string.IsNullOrEmpty(type))
            {
                return null;
            }


            if (type == RedisJobStoreSchema.TriggerTypeSimple)
            {
                var simpleTrigger = new SimpleTriggerImpl();

                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.RepeatCount]))
                {
                    simpleTrigger.RepeatCount = Convert.ToInt32(properties[RedisJobStoreSchema.RepeatCount]);

                }

                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.RepeatInterval]))
                {
                    simpleTrigger.RepeatInterval = TimeSpan.Parse(properties[RedisJobStoreSchema.RepeatInterval]);
                }

                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.TimesTriggered]))
                {
                    simpleTrigger.TimesTriggered = Convert.ToInt32(properties[RedisJobStoreSchema.TimesTriggered]);
                }

                PopulateTrigger(triggerKey, properties, simpleTrigger);

                return simpleTrigger;

            }
            else
            {

                var cronTrigger = new CronTriggerImpl();

                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.TimeZoneId]))
                {
                    cronTrigger.TimeZone =
                        TimeZoneInfo.FindSystemTimeZoneById(properties[RedisJobStoreSchema.TimeZoneId]);
                }
                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.CronExpression]))
                {
                    cronTrigger.CronExpressionString = properties[RedisJobStoreSchema.CronExpression];
                }


                PopulateTrigger(triggerKey, properties, cronTrigger);

                return cronTrigger;

            }


        }

        /// <summary>
        /// populate common properties of a trigger.
        /// </summary>
        /// <param name="triggerKey">triggerKey</param>
        /// <param name="properties">trigger's properties</param>
        /// <param name="trigger">IOperableTrigger</param>
        private void PopulateTrigger(TriggerKey triggerKey, IDictionary<string, string> properties, IOperableTrigger trigger)
        {
            trigger.Key = triggerKey;
            trigger.JobKey = RedisJobStoreSchema.JobKey(properties[RedisJobStoreSchema.JobHash]);
            trigger.Description = properties[RedisJobStoreSchema.Description];
            trigger.FireInstanceId = properties[RedisJobStoreSchema.FireInstanceId];
            trigger.CalendarName = properties[RedisJobStoreSchema.CalendarName];
            trigger.Priority = int.Parse(properties[RedisJobStoreSchema.Priority]);
            trigger.MisfireInstruction = int.Parse(properties[RedisJobStoreSchema.MisfireInstruction]);
            trigger.StartTimeUtc = DateTimeFromUnixTimestampMillis(
                                           double.Parse(properties[RedisJobStoreSchema.StartTime]));

            trigger.EndTimeUtc = string.IsNullOrEmpty(properties[RedisJobStoreSchema.EndTime])
                                      ? default(DateTimeOffset?)
                                      : DateTimeFromUnixTimestampMillis(
                                          double.Parse(properties[RedisJobStoreSchema.EndTime]));

            var baseTrigger = trigger as AbstractTrigger;

            if (baseTrigger != null)
            {
                trigger.SetNextFireTimeUtc(string.IsNullOrEmpty(properties[RedisJobStoreSchema.NextFireTime])
                                      ? default(DateTimeOffset?)
                                      : DateTimeFromUnixTimestampMillis(
                                          double.Parse(properties[RedisJobStoreSchema.NextFireTime])));

                trigger.SetPreviousFireTimeUtc(string.IsNullOrEmpty(properties[RedisJobStoreSchema.PrevFireTime])
                                      ? default(DateTimeOffset?)
                                      : DateTimeFromUnixTimestampMillis(
                                          double.Parse(properties[RedisJobStoreSchema.PrevFireTime])));
            }

            // trigger job-data-map entries are populated separately by the outer RetrieveTrigger(TriggerKey)
            // overload from TriggerDataMapHashKey - there is no separate per-trigger-type job data map to read here.
        }

        /// <summary>
        /// convert to utc datetime
        /// </summary>
        /// <param name="millis">milliseconds</param>
        /// <returns>datetime in utc</returns>
        private static DateTime DateTimeFromUnixTimestampMillis(double millis)
        {
            return UnixEpoch.AddMilliseconds(millis);
        }

        /// <summary>
        /// get the total milli seconds from unix epoch time.
        /// </summary>
        /// <param name="dateTimeOffset">DateTimeOffset</param>
        /// <returns>total millisconds from epoch time</returns>
        public double ToUnixTimeMilliseconds(DateTimeOffset dateTimeOffset)
        {
            // Truncate sub-millisecond precision before offsetting by the Unix Epoch to avoid
            // the last digit being off by one for dates that result in negative Unix times
            return (dateTimeOffset - new DateTimeOffset(UnixEpoch)).TotalMilliseconds;
        }

        /// <summary>
        /// try to acquire a named redis lock.
        /// </summary>
        /// <param name="lockKey">the redis key backing this lock.</param>
        /// <returns>the acquired lock's token, or null if the lock could not be acquired.</returns>
        private async Task<string> TryLock(string lockKey)
        {
            var guid = Guid.NewGuid().ToString();
            var lockAcquired = await Db.LockTakeAsync(lockKey, guid, TimeSpan.FromMilliseconds(RedisLockTimeout)).ConfigureAwait(false);
            return lockAcquired ? guid : null;
        }

        /// <summary>
        /// get a random integer within the specified bounds.
        /// </summary>
        /// <param name="min">mininum number</param>
        /// <param name="max">maxinum number></param>
        /// <returns>random number</returns>
        private static readonly Random _random = new Random();

        protected int RandomInt(int min, int max)
        {
            return _random.Next((max - min) + 1) + min;
        }

        /// <summary>
        /// waits for the named lock without blocking a thread-pool thread for the whole contention period
        /// (uses <see cref="Task.Delay(int)"/> between retries, and every Redis call involved is issued via
        /// the async API). Use this for every lock-critical section, since even brief contention would
        /// otherwise tie up a worker thread doing nothing but waiting.
        /// </summary>
        /// <param name="lockKey">the redis key backing this lock.</param>
        /// <returns>the token that must be passed to <see cref="UnlockAsync"/> to release this lock.</returns>
        public async Task<string> LockWithWaitAsync(string lockKey)
        {
            string lockValue;
            while ((lockValue = await TryLock(lockKey).ConfigureAwait(false)) == null)
            {
                _logger.Info("waiting for redis lock");
                await Task.Delay(RandomInt(75, 125)).ConfigureAwait(false);
            }

            return lockValue;
        }

        /// <summary>
        /// release a named lock.
        /// </summary>
        /// <param name="lockKey">the redis key backing this lock.</param>
        /// <param name="lockValue">the token returned by the matching <see cref="LockWithWaitAsync"/> call.</param>
        /// <returns>unlock succeeds or not</returns>
        public async Task<bool> UnlockAsync(string lockKey, string lockValue)
        {
            var released = await Db.LockReleaseAsync(lockKey, lockValue).ConfigureAwait(false);
            if (!released)
            {
                _logger.WarnFormat("lock {0} was no longer held on release - it likely expired while held and was taken over by another caller", lockKey);
            }
            return released;
        }

        /// <summary>
        /// return the logger for the current class
        /// </summary>
        protected ILog Logger
        {
            get { return _logger; }
        }

        #endregion

    }
}
