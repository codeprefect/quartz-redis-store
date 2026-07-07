using Quartz;
using Quartz.Impl.Matchers;
using Quartz.Spi;
using StackExchange.Redis;

namespace QuartzRedis.Store
{
    /// <summary>
    /// Master/slave redis storage for job, trigger, calendar, scheduler related operations.
    /// </summary>
    public class RedisStorage : BaseJobStorage
    {

        #region constructor
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="redisJobStoreSchema">RedisJobStoreSchema</param>
        /// <param name="db">IDatabase</param>
        /// <param name="signaler">ISchedulerSignaler</param>
        /// <param name="schedulerInstanceId">SchedulerInstanceId</param>
        /// <param name="triggerLockTimeout">triggerLockTimeout</param>
        /// <param name="redisLockTimeout">redisLockTimeout</param>
        public RedisStorage(RedisJobStoreSchema redisJobStoreSchema, IDatabase db, ISchedulerSignaler signaler, string schedulerInstanceId, int triggerLockTimeout, int redisLockTimeout) : base(redisJobStoreSchema, db, signaler, schedulerInstanceId, triggerLockTimeout, redisLockTimeout) { }

        #endregion

        #region Overrides of BaseJobStorage
        /// <summary>
        /// Store the given <see cref="T:Quartz.IJobDetail"/>.
        /// </summary>
        /// <param name="jobDetail">The <see cref="T:Quartz.IJobDetail"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.IJob"/> existing in the
        ///             <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should be
        ///             over-written.
        ///             </param>
        public override async Task StoreJob(IJobDetail jobDetail, bool replaceExisting)
        {
            var jobHashKey = RedisJobStoreSchema.JobHashKey(jobDetail.Key);
            var jobDataMapHashKey = RedisJobStoreSchema.JobDataMapHashKey(jobDetail.Key);
            var jobGroupSetKey = RedisJobStoreSchema.JobGroupSetKey(jobDetail.Key.Group);

            if (await Db.KeyExistsAsync(jobHashKey).ConfigureAwait(false) && !replaceExisting)
            {
                throw new ObjectAlreadyExistsException(jobDetail);
            }

            await Db.HashSetAsync(jobHashKey, ConvertToHashEntries(jobDetail)).ConfigureAwait(false);

            await Db.HashSetAsync(jobDataMapHashKey, ConvertToHashEntries(jobDetail.JobDataMap)).ConfigureAwait(false);

            await Db.SetAddAsync(RedisJobStoreSchema.JobsSetKey(), jobHashKey).ConfigureAwait(false);

            await Db.SetAddAsync(RedisJobStoreSchema.JobGroupsSetKey(), jobGroupSetKey).ConfigureAwait(false);

            await Db.SetAddAsync(jobGroupSetKey, jobHashKey).ConfigureAwait(false);

        }

        /// <summary>
        /// Store the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <param name="trigger">The <see cref="T:Quartz.ITrigger"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.ITrigger"/> existing in
        ///             the <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should
        ///             be over-written.</param><throws>ObjectAlreadyExistsException </throws>
        public override async Task StoreTrigger(ITrigger trigger, bool replaceExisting)
        {
            var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(trigger.Key);
            var triggerGroupSetKey = RedisJobStoreSchema.TriggerGroupSetKey(trigger.Key.Group);
            var jobTriggerSetKey = RedisJobStoreSchema.JobTriggersSetKey(trigger.JobKey);

            if (((trigger is ISimpleTrigger) == false) && ((trigger is ICronTrigger) == false))
            {
                throw new NotImplementedException("Unknown trigger, only SimpleTrigger and CronTrigger are supported");
            }

            var triggerExists = await Db.KeyExistsAsync(triggerHashKey).ConfigureAwait(false);

            if (triggerExists && replaceExisting == false)
            {
                throw new ObjectAlreadyExistsException(trigger);
            }


            await Db.HashSetAsync(triggerHashKey, ConvertToHashEntries(trigger)).ConfigureAwait(false);
            await Db.HashSetAsync(RedisJobStoreSchema.TriggerDataMapHashKey(trigger.Key), ConvertToHashEntries(((IOperableTrigger)trigger).JobDataMap)).ConfigureAwait(false);
            await Db.SetAddAsync(RedisJobStoreSchema.TriggersSetKey(), triggerHashKey).ConfigureAwait(false);
            await Db.SetAddAsync(RedisJobStoreSchema.TriggerGroupsSetKey(), triggerGroupSetKey).ConfigureAwait(false);
            await Db.SetAddAsync(triggerGroupSetKey, triggerHashKey).ConfigureAwait(false);
            await Db.SetAddAsync(jobTriggerSetKey, triggerHashKey).ConfigureAwait(false);

            if (!string.IsNullOrEmpty(trigger.CalendarName))
            {
                var calendarTriggersSetKey = RedisJobStoreSchema.CalendarTriggersSetKey(trigger.CalendarName);
                await Db.SetAddAsync(calendarTriggersSetKey, triggerHashKey).ConfigureAwait(false);
            }

            //if trigger already exists, remove it from all the possible states.
            if (triggerExists)
            {
                await this.UnsetTriggerState(triggerHashKey).ConfigureAwait(false);
            }

            //then update it with the new state in the its respectvie sorted set.
            await UpdateTriggerState(trigger).ConfigureAwait(false);
        }

        /// <summary>
        /// remove the trigger from all the possible state in the its respective sorted set.
        /// </summary>
        /// <param name="triggerKey">trigger key</param>
        /// <returns>succeeds or not</returns>
        public override Task<bool> UnsetTriggerState(TriggerKey triggerKey)
        {
            return UnsetTriggerState(RedisJobStoreSchema.TriggerHashkey(triggerKey));
        }

        /// <summary>
        /// remove the trigger from all the possible state in the its respective sorted set.
        /// </summary>
        /// <param name="triggerHashKey">trigger hash key</param>
        /// <returns>succeeds or not</returns>
        public override async Task<bool> UnsetTriggerState(string triggerHashKey)
        {
            var removed = false;
            foreach (RedisTriggerState state in Enum.GetValues(typeof(RedisTriggerState)))
            {
                if (await Db.SortedSetRemoveAsync(RedisJobStoreSchema.TriggerStateSetKey(state), triggerHashKey).ConfigureAwait(false))
                {
                    removed = true;
                }
            }

            if (removed)
            {
                await Db.HashDeleteAsync(triggerHashKey, RedisJobStoreSchema.CurrentState).ConfigureAwait(false);
                return await Db.KeyDeleteAsync(RedisJobStoreSchema.TriggerLockKey(RedisJobStoreSchema.TriggerKey(triggerHashKey))).ConfigureAwait(false);
            }

            return false;
        }


        /// <summary>
        /// Store the given <see cref="T:Quartz.ICalendar"/>.
        /// </summary>
        /// <param name="name">The name.</param><param name="calendar">The <see cref="T:Quartz.ICalendar"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.ICalendar"/> existing
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group
        ///             should be over-written.</param><param name="updateTriggers">If <see langword="true"/>, any <see cref="T:Quartz.ITrigger"/>s existing
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/> that reference an existing
        ///             Calendar with the same name with have their next fire time
        ///             re-computed with the new <see cref="T:Quartz.ICalendar"/>.</param><throws>ObjectAlreadyExistsException </throws>
        public override async Task StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            string calendarHashKey = RedisJobStoreSchema.CalendarHashKey(name);

            if (replaceExisting == false && await Db.KeyExistsAsync(calendarHashKey).ConfigureAwait(false))
            {
                throw new ObjectAlreadyExistsException(string.Format("Calendar with key {0} already exists", calendarHashKey));
            }

            await Db.HashSetAsync(calendarHashKey, ConvertToHashEntries(calendar)).ConfigureAwait(false);
            await Db.SetAddAsync(RedisJobStoreSchema.CalendarsSetKey(), calendarHashKey).ConfigureAwait(false);

            if (updateTriggers)
            {
                var calendarTriggersSetkey = RedisJobStoreSchema.CalendarTriggersSetKey(name);

                var triggerHashKeys = await Db.SetMembersAsync(calendarTriggersSetkey).ConfigureAwait(false);

                foreach (var triggerHashKey in triggerHashKeys)
                {
                    var trigger = await RetrieveTrigger(RedisJobStoreSchema.TriggerKey(triggerHashKey)).ConfigureAwait(false);

                    if (trigger == null)
                    {
                        continue;
                    }

                    trigger.UpdateWithNewCalendar(calendar, TimeSpan.FromMilliseconds(MisfireThreshold));

                    await StoreTrigger(trigger, true).ConfigureAwait(false);
                }
            }

        }

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
        public override async Task<bool> RemoveCalendar(string calendarName)
        {
            var calendarTriggersSetKey = RedisJobStoreSchema.CalendarTriggersSetKey(calendarName);

            if (await Db.SetLengthAsync(calendarTriggersSetKey).ConfigureAwait(false) > 0)
            {
                throw new JobPersistenceException(string.Format("There are triggers are using calendar {0}",
                                                                calendarName));
            }

            var calendarHashKey = RedisJobStoreSchema.CalendarHashKey(calendarName);

            return await Db.KeyDeleteAsync(calendarHashKey).ConfigureAwait(false) && await Db.SetRemoveAsync(RedisJobStoreSchema.CalendarsSetKey(), calendarHashKey).ConfigureAwait(false);
        }

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
        public override async Task<bool> RemoveJob(JobKey jobKey)
        {
            var jobHashKey = RedisJobStoreSchema.JobHashKey(jobKey);
            var jobDataMapHashKey = RedisJobStoreSchema.JobDataMapHashKey(jobKey);
            var jobGroupSetKey = RedisJobStoreSchema.JobGroupSetKey(jobKey.Group);
            var jobTriggerSetKey = RedisJobStoreSchema.JobTriggersSetKey(jobKey);

            var delJobHashKeyResult = await Db.KeyDeleteAsync(jobHashKey).ConfigureAwait(false);

            await Db.KeyDeleteAsync(jobDataMapHashKey).ConfigureAwait(false);

            await Db.SetRemoveAsync(RedisJobStoreSchema.JobsSetKey(), jobHashKey).ConfigureAwait(false);

            await Db.SetRemoveAsync(jobGroupSetKey, jobHashKey).ConfigureAwait(false);

            var jobTriggerSetResult = await Db.SetMembersAsync(jobTriggerSetKey).ConfigureAwait(false);

            await Db.KeyDeleteAsync(jobTriggerSetKey).ConfigureAwait(false);

            var jobGroupSetLengthResult = await Db.SetLengthAsync(jobGroupSetKey).ConfigureAwait(false);

            if (jobGroupSetLengthResult == 0)
            {
                await Db.SetRemoveAsync(RedisJobStoreSchema.JobGroupsSetKey(), jobGroupSetKey).ConfigureAwait(false);
            }

            // remove all triggers associated with this job
            foreach (var triggerHashKey in jobTriggerSetResult)
            {
                var triggerkey = RedisJobStoreSchema.TriggerKey(triggerHashKey);
                var triggerGroupKey = RedisJobStoreSchema.TriggerGroupSetKey(triggerkey.Group);

                await this.UnsetTriggerState(triggerHashKey).ConfigureAwait(false);

                await Db.SetRemoveAsync(RedisJobStoreSchema.TriggersSetKey(), triggerHashKey).ConfigureAwait(false);

                await Db.SetRemoveAsync(triggerGroupKey, triggerHashKey).ConfigureAwait(false);

                if (await Db.SetLengthAsync(triggerGroupKey).ConfigureAwait(false) == 0)
                {
                    await Db.SetRemoveAsync(RedisJobStoreSchema.TriggerGroupsSetKey(), triggerGroupKey).ConfigureAwait(false);
                }

                var calendarName = await Db.HashGetAsync(triggerHashKey.ToString(), RedisJobStoreSchema.CalendarName).ConfigureAwait(false);
                if (!string.IsNullOrEmpty(calendarName))
                {
                    await Db.SetRemoveAsync(RedisJobStoreSchema.CalendarTriggersSetKey(calendarName), triggerHashKey).ConfigureAwait(false);
                }

                await Db.KeyDeleteAsync(RedisJobStoreSchema.TriggerDataMapHashKey(triggerkey)).ConfigureAwait(false);
                await Db.KeyDeleteAsync(triggerHashKey.ToString()).ConfigureAwait(false);
            }

            return delJobHashKeyResult;
        }


        /// <summary>
        /// Pause the <see cref="T:Quartz.IJob"/> with the given key - by
        ///             pausing all of its current <see cref="T:Quartz.ITrigger"/>s.
        /// </summary>
        public override async Task<IReadOnlyCollection<string>> PauseJobs(GroupMatcher<JobKey> matcher)
        {
            var pausedJobGroups = new List<string>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var jobGroupSetKey = RedisJobStoreSchema.JobGroupSetKey(matcher.CompareToValue);

                if (await Db.SetAddAsync(RedisJobStoreSchema.PausedJobGroupsSetKey(), jobGroupSetKey).ConfigureAwait(false))
                {
                    pausedJobGroups.Add(RedisJobStoreSchema.JobGroup(jobGroupSetKey));

                    foreach (RedisValue val in await Db.SetMembersAsync(jobGroupSetKey).ConfigureAwait(false))
                    {
                        await PauseJob(RedisJobStoreSchema.JobKey(val)).ConfigureAwait(false);
                    }
                }
            }
            else
            {
                var jobGroupSets = await Db.SetMembersAsync(RedisJobStoreSchema.JobGroupsSetKey()).ConfigureAwait(false);

                foreach (var jobGroupSet in jobGroupSets)
                {
                    if (!matcher.CompareWithOperator.Evaluate(RedisJobStoreSchema.JobGroup(jobGroupSet), matcher.CompareToValue))
                    {
                        continue;
                    }

                    if (await Db.SetAddAsync(RedisJobStoreSchema.PausedJobGroupsSetKey(), jobGroupSet).ConfigureAwait(false))
                    {
                        pausedJobGroups.Add(RedisJobStoreSchema.JobGroup(jobGroupSet));

                        foreach (var jobHashKey in await Db.SetMembersAsync(jobGroupSet.ToString()).ConfigureAwait(false))
                        {
                            await PauseJob(RedisJobStoreSchema.JobKey(jobHashKey)).ConfigureAwait(false);
                        }
                    }
                }
            }

            return pausedJobGroups;
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
        public override async Task<IReadOnlyCollection<string>> ResumeJobs(GroupMatcher<JobKey> matcher)
        {
            var resumedJobGroups = new List<string>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var jobGroupSetKey = RedisJobStoreSchema.JobGroupSetKey(matcher.CompareToValue);

                var removedPausedResult = await Db.SetRemoveAsync(RedisJobStoreSchema.PausedJobGroupsSetKey(), jobGroupSetKey).ConfigureAwait(false);
                var jobsResult = await Db.SetMembersAsync(jobGroupSetKey).ConfigureAwait(false);


                if (removedPausedResult)
                {
                    resumedJobGroups.Add(RedisJobStoreSchema.JobGroup(jobGroupSetKey));
                }

                foreach (var job in jobsResult)
                {
                    await ResumeJob(RedisJobStoreSchema.JobKey(job)).ConfigureAwait(false);
                }
            }
            else
            {
                foreach (var jobGroupSetKey in await Db.SetMembersAsync(RedisJobStoreSchema.JobGroupsSetKey()).ConfigureAwait(false))
                {
                    if (matcher.CompareWithOperator.Evaluate(RedisJobStoreSchema.JobGroup(jobGroupSetKey),
                                                            matcher.CompareToValue))
                    {

                        resumedJobGroups.AddRange(await ResumeJobs(
                                GroupMatcher<JobKey>.GroupEquals(RedisJobStoreSchema.JobGroup(jobGroupSetKey))).ConfigureAwait(false));
                    }
                }
            }

            return new global::System.Collections.Generic.HashSet<string>(resumedJobGroups);
        }

        /// <summary>
        /// Resume (un-pause) the <see cref="T:Quartz.ITrigger"/> with the
        ///             given key.
        /// <para>
        /// If the <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="T:System.String"/>
        public override async Task ResumeTrigger(TriggerKey triggerKey)
        {
            var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(triggerKey);

            var triggerExists = await Db.SetContainsAsync(RedisJobStoreSchema.TriggersSetKey(), triggerHashKey).ConfigureAwait(false);

            var isPausedTrigger =
                await Db.SortedSetScoreAsync(RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Paused),
                                         triggerHashKey).ConfigureAwait(false);

            var isPausedBlockedTrigger =
                await Db.SortedSetScoreAsync(RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.PausedBlocked),
                                         triggerHashKey).ConfigureAwait(false);

            if (triggerExists == false)
            {
                return;
            }

            //Trigger is not paused, cant be resumed then.
            if (!isPausedTrigger.HasValue && !isPausedBlockedTrigger.HasValue)
            {
                return;
            }

            var trigger = await RetrieveTrigger(triggerKey).ConfigureAwait(false);

            var jobHashKey = RedisJobStoreSchema.JobHashKey(trigger.JobKey);

            var nextFireTime = trigger.GetNextFireTimeUtc();

            if (nextFireTime.HasValue)
            {

                if (await Db.SetContainsAsync(RedisJobStoreSchema.BlockedJobsSet(), jobHashKey).ConfigureAwait(false))
                {
                    await SetTriggerState(RedisTriggerState.Blocked, nextFireTime.Value.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey).ConfigureAwait(false);
                }
                else
                {
                    await SetTriggerState(RedisTriggerState.Waiting, nextFireTime.Value.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey).ConfigureAwait(false);
                }
            }
            else
            {
                // no more fire times left - it can't stay parked in Paused/PausedBlocked forever, and
                // ApplyMisfire below is a no-op when there's no next fire time, so it can't rescue it either.
                await SetTriggerState(RedisTriggerState.Completed, DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey).ConfigureAwait(false);
                SchedulerSignaler.NotifySchedulerListenersFinalized(trigger);
            }
            await ApplyMisfire(trigger).ConfigureAwait(false);
        }

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
        public override async Task<bool> RemoveTrigger(TriggerKey triggerKey, bool removeNonDurableJob = true)
        {
            var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(triggerKey);

            if (!await Db.KeyExistsAsync(triggerHashKey).ConfigureAwait(false))
            {
                return false;
            }

            IOperableTrigger trigger = await RetrieveTrigger(triggerKey).ConfigureAwait(false);

            var triggerGroupSetKey = RedisJobStoreSchema.TriggerGroupSetKey(triggerKey.Group);
            var jobHashKey = RedisJobStoreSchema.JobHashKey(trigger.JobKey);
            var jobTriggerSetkey = RedisJobStoreSchema.JobTriggersSetKey(trigger.JobKey);

            await Db.SetRemoveAsync(RedisJobStoreSchema.TriggersSetKey(), triggerHashKey).ConfigureAwait(false);

            await Db.SetRemoveAsync(triggerGroupSetKey, triggerHashKey).ConfigureAwait(false);

            await Db.SetRemoveAsync(jobTriggerSetkey, triggerHashKey).ConfigureAwait(false);

            if (await Db.SetLengthAsync(triggerGroupSetKey).ConfigureAwait(false) == 0)
            {
                await Db.SetRemoveAsync(RedisJobStoreSchema.TriggerGroupsSetKey(), triggerGroupSetKey).ConfigureAwait(false);
            }

            if (removeNonDurableJob)
            {

                var jobTriggerSetKeyLengthResult = await Db.SetLengthAsync(jobTriggerSetkey).ConfigureAwait(false);

                var jobExistsResult = await Db.KeyExistsAsync(jobHashKey).ConfigureAwait(false);

                if (jobTriggerSetKeyLengthResult == 0 && jobExistsResult)
                {
                    var job = await RetrieveJob(trigger.JobKey).ConfigureAwait(false);

                    if (job.Durable == false)
                    {
                        await RemoveJob(job.Key).ConfigureAwait(false);
                        SchedulerSignaler.NotifySchedulerListenersJobDeleted(job.Key);
                    }
                }
            }

            if (!string.IsNullOrEmpty(trigger.CalendarName))
            {
                await Db.SetRemoveAsync(RedisJobStoreSchema.CalendarTriggersSetKey(trigger.CalendarName), triggerHashKey).ConfigureAwait(false);
            }

            await this.UnsetTriggerState(triggerHashKey).ConfigureAwait(false);
            await Db.KeyDeleteAsync(RedisJobStoreSchema.TriggerDataMapHashKey(triggerKey)).ConfigureAwait(false);
            return await Db.KeyDeleteAsync(triggerHashKey).ConfigureAwait(false);
        }

        /// <summary>
        /// Resume (un-pause) all of the <see cref="T:Quartz.ITrigger"/>s
        ///             in the given group.
        /// <para>
        /// If any <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        public override async Task<IReadOnlyCollection<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher)
        {
            var resumedTriggerGroups = new List<string>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var triggerGroupSetKey =
                    RedisJobStoreSchema.TriggerGroupSetKey(matcher.CompareToValue);

                await Db.SetRemoveAsync(RedisJobStoreSchema.PausedTriggerGroupsSetKey(), triggerGroupSetKey).ConfigureAwait(false);

                var triggerHashKeysResult = await Db.SetMembersAsync(triggerGroupSetKey).ConfigureAwait(false);

                foreach (var triggerHashKey in triggerHashKeysResult)
                {
                    var trigger = await RetrieveTrigger(RedisJobStoreSchema.TriggerKey(triggerHashKey)).ConfigureAwait(false);

                    if (trigger == null)
                    {
                        continue;
                    }

                    await ResumeTrigger(trigger.Key).ConfigureAwait(false);

                    if (!resumedTriggerGroups.Contains(trigger.Key.Group))
                    {
                        resumedTriggerGroups.Add(trigger.Key.Group);
                    }
                }
            }
            else
            {
                foreach (var triggerGroupSetKy in await Db.SetMembersAsync(RedisJobStoreSchema.TriggerGroupsSetKey()).ConfigureAwait(false))
                {
                    if (matcher.CompareWithOperator.Evaluate(RedisJobStoreSchema.TriggerGroup(triggerGroupSetKy),
                                                            matcher.CompareToValue))
                    {
                        resumedTriggerGroups.AddRange(await ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(RedisJobStoreSchema.TriggerGroup(triggerGroupSetKy))).ConfigureAwait(false));
                    }
                }
            }


            return resumedTriggerGroups;
        }

        /// <summary>
        /// Pause the <see cref="T:Quartz.ITrigger"/> with the given key.
        /// </summary>
        public override async Task PauseTrigger(TriggerKey triggerKey)
        {
            var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(triggerKey);

            var triggerExistsResult = await Db.KeyExistsAsync(triggerHashKey).ConfigureAwait(false);

            var completedScoreResult =
                await Db.SortedSetScoreAsync(RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Completed),
                                        triggerHashKey).ConfigureAwait(false);

            var nextFireTimeResult = await Db.HashGetAsync(triggerHashKey, RedisJobStoreSchema.NextFireTime).ConfigureAwait(false);

            var blockedScoreResult =
                await Db.SortedSetScoreAsync(RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Blocked),
                                        triggerHashKey).ConfigureAwait(false);


            if (!triggerExistsResult)
            {
                return;
            }

            if (completedScoreResult.HasValue)
            {
                return;
            }

            var nextFireTime = double.Parse(string.IsNullOrEmpty(nextFireTimeResult) ? "-1" : nextFireTimeResult.ToString());

            if (blockedScoreResult.HasValue)
            {
                await SetTriggerState(RedisTriggerState.PausedBlocked, nextFireTime, triggerHashKey).ConfigureAwait(false);
            }
            else
            {
                await SetTriggerState(RedisTriggerState.Paused, nextFireTime, triggerHashKey).ConfigureAwait(false);
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
        public override async Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(IReadOnlyCollection<IOperableTrigger> triggers)
        {
            var result = new List<TriggerFiredResult>();

            foreach (var trigger in triggers)
            {
                var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(trigger.Key);

                var triggerExistResult = await Db.KeyExistsAsync(triggerHashKey).ConfigureAwait(false);
                var triggerAcquiredResult =
                    await Db.SortedSetScoreAsync(RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Acquired),
                                             triggerHashKey).ConfigureAwait(false);

                if (triggerExistResult == false)
                {
                    Logger.WarnFormat("Trigger {0} does not exist", triggerHashKey);
                    continue;
                }

                if (!triggerAcquiredResult.HasValue)
                {
                    Logger.WarnFormat("Trigger {0} was not acquired", triggerHashKey);
                    continue;
                }

                ICalendar calendar = null;

                bool triggerflag = true;

                string calendarname = trigger.CalendarName;
                if (!string.IsNullOrEmpty(calendarname))
                {
                    calendar = await this.RetrieveCalendar(calendarname).ConfigureAwait(false);

                    if (calendar == null)
                    {
                        continue;
                    }
                }

                var previousFireTime = trigger.GetPreviousFireTimeUtc();

                trigger.Triggered(calendar);

                var job = await this.RetrieveJob(trigger.JobKey).ConfigureAwait(false);

                if (job == null)
                {
                    Logger.WarnFormat("Job for trigger {0} no longer exists", triggerHashKey);
                    continue;
                }

                var triggerFireBundle = new TriggerFiredBundle(job, trigger, calendar, false, DateTimeOffset.UtcNow,
                                                               previousFireTime, previousFireTime, trigger.GetNextFireTimeUtc());

                if (job.ConcurrentExecutionDisallowed)
                {
                    var jobHasKey = this.RedisJobStoreSchema.JobHashKey(trigger.JobKey);
                    var jobTriggerSetKey = this.RedisJobStoreSchema.JobTriggersSetKey(job.Key);

                    foreach (var nonConcurrentTriggerHashKey in await this.Db.SetMembersAsync(jobTriggerSetKey).ConfigureAwait(false))
                    {
                        var score =
                            await this.Db.SortedSetScoreAsync(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Waiting),
                                                    nonConcurrentTriggerHashKey).ConfigureAwait(false);

                        if (score.HasValue)
                        {
                            await this.SetTriggerState(RedisTriggerState.Blocked, score.Value, nonConcurrentTriggerHashKey, RedisTriggerState.Waiting).ConfigureAwait(false);
                        }
                        else
                        {
                            score = await this.Db.SortedSetScoreAsync(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Paused),
                                                    nonConcurrentTriggerHashKey).ConfigureAwait(false);
                            if (score.HasValue)
                            {
                                await this.SetTriggerState(RedisTriggerState.PausedBlocked, score.Value, nonConcurrentTriggerHashKey, RedisTriggerState.Paused).ConfigureAwait(false);
                            }
                        }
                    }


                    await Db.SetAddAsync(this.RedisJobStoreSchema.JobBlockedKey(job.Key), this.SchedulerInstanceId).ConfigureAwait(false);

                    await Db.SetAddAsync(this.RedisJobStoreSchema.BlockedJobsSet(), jobHasKey).ConfigureAwait(false);
                }

                //release the fired triggers
                var nextFireTimeUtc = trigger.GetNextFireTimeUtc();
                if (nextFireTimeUtc != null)
                {
                    var nextFireTime = nextFireTimeUtc.Value;
                    await this.Db.HashSetAsync(triggerHashKey, RedisJobStoreSchema.NextFireTime, nextFireTime.DateTime.ToUnixTimeMilliSeconds()).ConfigureAwait(false);
                    await this.SetTriggerState(RedisTriggerState.Waiting, nextFireTime.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey, RedisTriggerState.Acquired).ConfigureAwait(false);


                    double? oldscore = await this.Db.SortedSetScoreAsync(this.RedisJobStoreSchema.JobBlockedKeyTime(job.Key),
                                            triggerHashKey).ConfigureAwait(false);
                    if ((oldscore ?? 0) == nextFireTime.DateTime.ToUnixTimeMilliSeconds())
                    {
                        triggerflag = false;
                    }
                    await this.Db.SortedSetAddAsync(this.RedisJobStoreSchema.JobBlockedKeyTime(job.Key),
                                        triggerHashKey, nextFireTime.DateTime.ToUnixTimeMilliSeconds()).ConfigureAwait(false);

                }
                else
                {
                    await this.Db.HashSetAsync(triggerHashKey, RedisJobStoreSchema.NextFireTime, "").ConfigureAwait(false);
                    await this.UnsetTriggerState(triggerHashKey).ConfigureAwait(false);
                }

                if (triggerflag)
                {
                    result.Add(new TriggerFiredResult(triggerFireBundle));
                }
                else
                {
                    result.Add(new TriggerFiredResult(new Exception("trigger is locked")));
                }

            }

            return result;
        }

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> that the scheduler has completed the
        ///             firing of the given <see cref="T:Quartz.ITrigger"/> (and the execution its
        ///             associated <see cref="T:Quartz.IJob"/>), and that the <see cref="T:Quartz.JobDataMap"/>
        ///             in the given <see cref="T:Quartz.IJobDetail"/> should be updated if the <see cref="T:Quartz.IJob"/>
        ///             is stateful.
        /// </summary>
        public override async Task TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            var jobHashKey = this.RedisJobStoreSchema.JobHashKey(jobDetail.Key);

            var jobDataMapHashKey = this.RedisJobStoreSchema.JobDataMapHashKey(jobDetail.Key);

            var triggerHashKey = this.RedisJobStoreSchema.TriggerHashkey(trigger.Key);

            if (await this.Db.KeyExistsAsync(jobHashKey).ConfigureAwait(false))
            {
                Logger.InfoFormat("{0} - Job has completed", jobHashKey);

                if (jobDetail.PersistJobDataAfterExecution)
                {
                    var jobDataMap = jobDetail.JobDataMap;

                    await Db.KeyDeleteAsync(jobDataMapHashKey).ConfigureAwait(false);
                    if (jobDataMap != null && !jobDataMap.IsEmpty)
                    {
                        await Db.HashSetAsync(jobDataMapHashKey, ConvertToHashEntries(jobDataMap)).ConfigureAwait(false);
                    }

                }

                if (jobDetail.ConcurrentExecutionDisallowed)
                {

                    await Db.SetRemoveAsync(this.RedisJobStoreSchema.BlockedJobsSet(), jobHashKey).ConfigureAwait(false);

                    await Db.KeyDeleteAsync(this.RedisJobStoreSchema.JobBlockedKey(jobDetail.Key)).ConfigureAwait(false);

                    var jobTriggersSetKey = this.RedisJobStoreSchema.JobTriggersSetKey(jobDetail.Key);

                    foreach (var nonConcurrentTriggerHashKey in await this.Db.SetMembersAsync(jobTriggersSetKey).ConfigureAwait(false))
                    {
                        var score =
                            await this.Db.SortedSetScoreAsync(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Blocked),
                                                    nonConcurrentTriggerHashKey).ConfigureAwait(false);
                        if (score.HasValue)
                        {
                            await this.SetTriggerState(RedisTriggerState.Waiting, score.Value, nonConcurrentTriggerHashKey, RedisTriggerState.Blocked).ConfigureAwait(false);
                        }
                        else
                        {
                            score =
                                await this.Db.SortedSetScoreAsync(
                                    this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.PausedBlocked),
                                    nonConcurrentTriggerHashKey).ConfigureAwait(false);

                            if (score.HasValue)
                            {
                                await this.SetTriggerState(RedisTriggerState.Paused, score.Value, nonConcurrentTriggerHashKey, RedisTriggerState.PausedBlocked).ConfigureAwait(false);
                            }
                        }
                    }
                    this.SchedulerSignaler.SignalSchedulingChange(null);
                }

            }
            else
            {
                await this.Db.SetRemoveAsync(this.RedisJobStoreSchema.BlockedJobsSet(), jobHashKey).ConfigureAwait(false);
            }

            if (await this.Db.KeyExistsAsync(triggerHashKey).ConfigureAwait(false))
            {
                if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
                {
                    if (trigger.GetNextFireTimeUtc().HasValue == false)
                    {
                        if (string.IsNullOrEmpty(await this.Db.HashGetAsync(triggerHashKey, RedisJobStoreSchema.NextFireTime).ConfigureAwait(false)))
                        {
                            await RemoveTrigger(trigger.Key).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        await this.RemoveTrigger(trigger.Key).ConfigureAwait(false);
                        this.SchedulerSignaler.SignalSchedulingChange(null);
                    }
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                {
                    await this.SetTriggerState(RedisTriggerState.Completed, DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey).ConfigureAwait(false);
                    this.SchedulerSignaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                {
                    double score = trigger.GetNextFireTimeUtc().HasValue
                                       ? trigger.GetNextFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds() : 0;
                    await this.SetTriggerState(RedisTriggerState.Error, score, triggerHashKey).ConfigureAwait(false);
                    this.SchedulerSignaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                {
                    var jobTriggersSetKey = this.RedisJobStoreSchema.JobTriggersSetKey(jobDetail.Key);

                    foreach (var errorTriggerHashKey in await this.Db.SetMembersAsync(jobTriggersSetKey).ConfigureAwait(false))
                    {
                        var nextFireTime = await this.Db.HashGetAsync(errorTriggerHashKey.ToString(), RedisJobStoreSchema.NextFireTime).ConfigureAwait(false);
                        var score = string.IsNullOrEmpty(nextFireTime) ? 0 : double.Parse(nextFireTime);
                        await this.SetTriggerState(RedisTriggerState.Error, score, errorTriggerHashKey).ConfigureAwait(false);
                    }
                    this.SchedulerSignaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                {
                    var jobTriggerSetKey = this.RedisJobStoreSchema.JobTriggersSetKey(jobDetail.Key);

                    foreach (var completedTriggerHashKey in await this.Db.SetMembersAsync(jobTriggerSetKey).ConfigureAwait(false))
                    {
                        await this.SetTriggerState(RedisTriggerState.Completed, DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds(),
                                             completedTriggerHashKey).ConfigureAwait(false);
                    }

                    this.SchedulerSignaler.SignalSchedulingChange(null);
                }
            }
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
        public override async Task<IReadOnlyCollection<JobKey>> JobKeys(GroupMatcher<JobKey> matcher)
        {
            var jobKeys = new global::System.Collections.Generic.HashSet<JobKey>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var jobGroupSetKey = this.RedisJobStoreSchema.JobGroupSetKey(matcher.CompareToValue);
                var jobHashKeys = await this.Db.SetMembersAsync(jobGroupSetKey).ConfigureAwait(false);
                if (jobHashKeys != null)
                {
                    foreach (var jobHashKey in jobHashKeys)
                    {
                        jobKeys.Add(this.RedisJobStoreSchema.JobKey(jobHashKey));
                    }
                }
            }
            else
            {
                var jobGroupSets = await this.Db.SetMembersAsync(this.RedisJobStoreSchema.JobGroupsSetKey()).ConfigureAwait(false);

                var matchingGroupKeys = jobGroupSets.Where(groupSet => matcher.CompareWithOperator.Evaluate(this.RedisJobStoreSchema.JobGroup(groupSet), matcher.CompareToValue)).ToList();

                // pipeline the per-group SetMembers calls into a single network round trip instead of one per group.
                var pendingMembers = await Task.WhenAll(matchingGroupKeys.Select(groupKey => Db.SetMembersAsync(groupKey.ToString()))).ConfigureAwait(false);

                foreach (var jobHashKeys in pendingMembers.Where(jobHashKeys => jobHashKeys != null))
                {
                    foreach (var jobHashKey in jobHashKeys)
                    {
                        jobKeys.Add(this.RedisJobStoreSchema.JobKey(jobHashKey));
                    }
                }
            }

            return jobKeys;
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ITrigger"/>s
        ///             that have the given group name.
        /// <para>
        /// If there are no triggers in the given group name, the result should be a
        ///             zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public override async Task<IReadOnlyCollection<TriggerKey>> TriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            var triggerKeys = new global::System.Collections.Generic.HashSet<TriggerKey>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var triggerGroupSetKey =
                    this.RedisJobStoreSchema.TriggerGroupSetKey(matcher.CompareToValue);

                var triggers = await this.Db.SetMembersAsync(triggerGroupSetKey).ConfigureAwait(false);


                foreach (var trigger in triggers)
                {
                    triggerKeys.Add(this.RedisJobStoreSchema.TriggerKey(trigger));
                }
            }
            else
            {
                var triggerGroupSets = await this.Db.SetMembersAsync(this.RedisJobStoreSchema.TriggerGroupsSetKey()).ConfigureAwait(false);
                var matchingGroupKeys = triggerGroupSets.Where(groupSet => matcher.CompareWithOperator.Evaluate(this.RedisJobStoreSchema.TriggerGroup(groupSet), matcher.CompareToValue)).ToList();

                // pipeline the per-group SetMembers calls into a single network round trip instead of one per group.
                var pendingMembers = await Task.WhenAll(matchingGroupKeys.Select(groupKey => Db.SetMembersAsync(groupKey.ToString()))).ConfigureAwait(false);

                foreach (var triggerHashKeys in pendingMembers)
                {
                    if (triggerHashKeys != null)
                    {
                        foreach (var triggerHashKey in triggerHashKeys)
                        {
                            triggerKeys.Add(this.RedisJobStoreSchema.TriggerKey(triggerHashKey));
                        }
                    }
                }
            }

            return triggerKeys;
        }

        /// <summary>
        /// Get the current state of the identified <see cref="T:Quartz.ITrigger"/>. Prefers the cached
        /// <see cref="RedisJobStoreSchema.CurrentState"/> hash field (a single round trip) - kept in sync by
        /// every trigger-state mutation via SetTriggerState/UnsetTriggerState - and only falls back to
        /// scanning every state's sorted set when that field is absent (e.g. a trigger untouched since
        /// before this field existed, or one with genuinely no recorded state).
        /// </summary>
        /// <seealso cref="T:Quartz.TriggerState"/>
        public override async Task<TriggerState> GetTriggerState(TriggerKey triggerKey)
        {
            var triggerHashKey = this.RedisJobStoreSchema.TriggerHashkey(triggerKey);

            var cachedState = await this.Db.HashGetAsync(triggerHashKey, RedisJobStoreSchema.CurrentState).ConfigureAwait(false);
            if (!string.IsNullOrEmpty(cachedState) && Enum.TryParse<RedisTriggerState>(cachedState, out var redisState))
            {
                return MapToQuartzTriggerState(redisState);
            }

            if (
                await this.Db.SortedSetScoreAsync(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Paused),
                                        triggerHashKey).ConfigureAwait(false) != null ||
                await this.Db.SortedSetScoreAsync(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.PausedBlocked),
                                        triggerHashKey).ConfigureAwait(false) != null)
            {
                return TriggerState.Paused;
            }
            if (
                await this.Db.SortedSetScoreAsync(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Blocked), triggerHashKey).ConfigureAwait(false) != null)
            {
                return TriggerState.Blocked;
            }
            if (
                await this.Db.SortedSetScoreAsync(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Waiting),
                                       triggerHashKey).ConfigureAwait(false) != null || await this.Db.SortedSetScoreAsync(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Acquired), triggerHashKey).ConfigureAwait(false) != null)
            {
                return TriggerState.Normal;
            }
            if (
                await this.Db.SortedSetScoreAsync(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Completed),
                                       triggerHashKey).ConfigureAwait(false) != null)
            {
                return TriggerState.Complete;
            }
            if (
                await this.Db.SortedSetScoreAsync(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Error), triggerHashKey).ConfigureAwait(false)
                != null)
            {
                return TriggerState.Error;
            }

            return TriggerState.None;

        }

        /// <summary>
        /// maps the internal Redis-specific trigger state to the Quartz-facing <see cref="TriggerState"/>,
        /// matching the same mapping the fallback sorted-set scan below uses.
        /// </summary>
        private static TriggerState MapToQuartzTriggerState(RedisTriggerState state)
        {
            switch (state)
            {
                case RedisTriggerState.Paused:
                case RedisTriggerState.PausedBlocked:
                    return TriggerState.Paused;
                case RedisTriggerState.Blocked:
                    return TriggerState.Blocked;
                case RedisTriggerState.Waiting:
                case RedisTriggerState.Acquired:
                    return TriggerState.Normal;
                case RedisTriggerState.Completed:
                    return TriggerState.Complete;
                case RedisTriggerState.Error:
                    return TriggerState.Error;
                default:
                    return TriggerState.None;
            }
        }

        /// <summary>
        /// Pause all of the <see cref="T:Quartz.ITrigger"/>s in the
        ///             given group.
        /// </summary>
        /// <remarks>
        /// The JobStore should "remember" that the group is paused, and impose the
        ///             pause on any new triggers that are added to the group while the group is
        ///             paused.
        /// </remarks>
        public override async Task<IReadOnlyCollection<string>> PauseTriggers(GroupMatcher<TriggerKey> matcher)
        {
            var pausedTriggerGroups = new List<string>();
            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var triggerGroupSetKey = this.RedisJobStoreSchema.TriggerGroupSetKey(matcher.CompareToValue);
                var addResult = await this.Db.SetAddAsync(this.RedisJobStoreSchema.PausedTriggerGroupsSetKey(), triggerGroupSetKey).ConfigureAwait(false);

                if (addResult)
                {
                    foreach (var triggerHashKey in await this.Db.SetMembersAsync(triggerGroupSetKey).ConfigureAwait(false))
                    {
                        await this.PauseTrigger(this.RedisJobStoreSchema.TriggerKey(triggerHashKey)).ConfigureAwait(false);
                    }

                    pausedTriggerGroups.Add(this.RedisJobStoreSchema.TriggerGroup(triggerGroupSetKey));
                }
            }
            else
            {
                var allTriggerGroups = await this.Db.SetMembersAsync(this.RedisJobStoreSchema.TriggerGroupsSetKey()).ConfigureAwait(false);

                foreach (var groupHashKey in allTriggerGroups)
                {
                    if (!matcher.CompareWithOperator.Evaluate(this.RedisJobStoreSchema.TriggerGroup(groupHashKey), matcher.CompareToValue))
                    {
                        continue;
                    }

                    var addResult = await this.Db.SetAddAsync(this.RedisJobStoreSchema.PausedTriggerGroupsSetKey(), groupHashKey).ConfigureAwait(false);

                    if (addResult)
                    {
                        foreach (var triggerHashKey in await Db.SetMembersAsync(groupHashKey.ToString()).ConfigureAwait(false))
                        {
                            await this.PauseTrigger(this.RedisJobStoreSchema.TriggerKey(triggerHashKey)).ConfigureAwait(false);
                        }
                        pausedTriggerGroups.Add(this.RedisJobStoreSchema.TriggerGroup(groupHashKey));
                    }
                }

            }

            return pausedTriggerGroups;
        }

        /// <summary>
        /// update trigger state
        /// if the group the trigger is in or the group the trigger's job is in are paused, then we need to check whether its job is in the blocked group, if it's then set its state to PausedBlocked, else set it to blocked.
        /// else set the trigger to the normal state (waiting), conver the nextfiretime into UTC milliseconds, so it could be stored as score in the sorted set.
        /// </summary>
        /// <param name="trigger">ITrigger</param>
        private async Task UpdateTriggerState(ITrigger trigger)
        {
            var triggerPausedResult =
                await this.Db.SetContainsAsync(this.RedisJobStoreSchema.PausedTriggerGroupsSetKey(),
                                     this.RedisJobStoreSchema.TriggerGroupSetKey(trigger.Key.Group)).ConfigureAwait(false);
            var jobPausedResult = await this.Db.SetContainsAsync(this.RedisJobStoreSchema.PausedJobGroupsSetKey(),
                                                         this.RedisJobStoreSchema.JobGroupSetKey(trigger.JobKey.Group)).ConfigureAwait(false);

            if (triggerPausedResult || jobPausedResult)
            {
                double nextFireTime = trigger.GetNextFireTimeUtc().HasValue
                                                   ? trigger.GetNextFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds() : -1;

                var jobHashKey = this.RedisJobStoreSchema.JobHashKey(trigger.JobKey);

                if (await this.Db.SetContainsAsync(this.RedisJobStoreSchema.BlockedJobsSet(), jobHashKey).ConfigureAwait(false))
                {
                    await this.SetTriggerState(RedisTriggerState.PausedBlocked,
                        nextFireTime, this.RedisJobStoreSchema.TriggerHashkey(trigger.Key)).ConfigureAwait(false);
                }
                else
                {
                    await this.SetTriggerState(RedisTriggerState.Paused,
                        nextFireTime, this.RedisJobStoreSchema.TriggerHashkey(trigger.Key)).ConfigureAwait(false);
                }
            }
            else if (trigger.GetNextFireTimeUtc().HasValue)
            {
                await this.SetTriggerState(RedisTriggerState.Waiting,
                       trigger.GetNextFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds(), this.RedisJobStoreSchema.TriggerHashkey(trigger.Key)).ConfigureAwait(false);
            }
        }

        #endregion
    }
}
