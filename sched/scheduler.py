import numpy as np
import pandas as pd
import math
from itertools import islice

class Job(object):
    """
    A standard job.

    After a job arrival time has passed it is always schedulable. A job has a
    weight and a priority. After a job has executed for its specified execution
    time, it may release a finish event (to trigger other jobs), or restart
    itself periodically or after a certain time interval.
    """

    def __init__(self, thread, arrival, execution_time, period=0, restart=0, weight=1, prio=0, event=None):
        """
        Constructs a job.

        Parameters
        ----------
            thread : str
                name of the thread
            arrival : int
                arrival time in microseconds
            execution_time : int
                execution time in microseconds
            period : int, optional
                period (in microseconds) after which the job is triggered again
                (default: 0)
            restart : int, optional
                time interval (in microseconds) after which the job is restarted
                if this is 0, the job does not restart itself after completion
                (default: 0)
            weight: int, optional
                job weight (default: 1)
            prio : int, optional
                job priority, higher value means higher priority (default: 0)
            event : str, optional
                name of the finish event (default: None)
        """
        self.arrival        = int(arrival)
        self.execution_time = int(execution_time)
        self.restart_time   = int(restart)
        self.period         = int(period)
        self.thread         = thread
        self.executed_time  = 0
        self.weight         = int(weight)
        self.finish_event   = event
        self.priority       = int(prio)

    def runtime_left(self):
        return max(self.execution_time - self.executed_time, 0)

    def restart(self, completion_time):
        """
        Creates a new Job if this Job is restarted after completion. The new
        Job's arrival time is set to arrival time + period or
        completion time + restart time depending on whether a period or restart
        time is set.

        Parameters
        ----------
            completion_time: int
                completion time of this job

        Returns
        -------
            None or Job object
        """
        job = None
        if self.period > 0:
            new_arrival = self.arrival + self.period
            extra_time  = 0
            while new_arrival < completion_time:
                new_arrival += self.period
                extra_time  += self.execution_time

            job = Job(self.thread, new_arrival,
                      self.execution_time,
                      period=self.period,
                      restart=self.restart_time,
                      weight=self.weight,
                      prio=self.priority,
                      event=self.finish_event)

            # if we missed any activation, we increase the computation need
            if extra_time:
                job.executed_time = -extra_time

        elif self.restart_time > 0:
            job = Job(self.thread, completion_time + self.restart_time,
                      self.execution_time,
                      period=self.period,
                      restart=self.restart_time,
                      weight=self.weight,
                      prio=self.priority,
                      event=self.finish_event)

        return job

    def __repr__(self):
        return "%d-Thread%s(%f) %d/%d" % (self.arrival, self.thread, self.weight, self.executed_time, self.execution_time)


class WaitingJob(object):
    """ A waiting job is activated by another job's completion event."""

    def __init__(self, thread, start_event, execution_time, period=0, restart=0, weight=1, prio=0, event=None):
        """
        Constucts a WaitingJob.

        Parameters
        ----------
            thread : str
                name of the thread
            start_event : str
                name of the start event
            execution_time : int
                execution time in microseconds
            period : int, optional
                period (in microseconds) after which the job is triggered again
                (default: 0)
            restart : int, optional
                time interval (in microseconds) after which the job is restarted
                if this is 0, the job does not restart itself after completion
                (default: 0)
            weight: int, optional
                job weight (default: 1)
            prio : int, optional
                job priority, higher value means higher priority (default: 0)
            event : str, optional
                name of the finish event (default: None)
        """
        self.start_event    = start_event
        self.execution_time = int(execution_time)
        self.restart_time   = int(restart)
        self.period         = int(period)
        self.thread         = thread
        self.weight         = int(weight)
        self.finish_event   = event
        self.priority       = int(prio)

    def start(self, time):
        """Start this job at the given time and return the corresponding Job object."""
        return Job(self.thread, time, self.execution_time,
                   period=self.period,
                   restart=self.restart_time,
                   weight=self.weight,
                   event=self.finish_event,
                   prio=self.priority)


class Scheduler(object):
    """ Generic job scheduler """

    def __init__(self):
        """ Constructs a Scheduler """
        # current time
        self.time = 0

        # last time a job has been scheduled
        self.last_time = 0

        # reader object providing to-be-scheduled jobs
        self.reader   = None

        # next job from the reader
        self.next_job = None

        # dictionary of WaitingJob objects indexed by event name
        self.waiting_jobs = dict()

        # list of restarted jobs (waiting for their arrival time)
        self.restarted_jobs = list()

        # dictionary of arrival, start and completion times indexed by thread name
        self.latency_trace = dict()

        # resulting execution trace
        self.trace = list()

        # list/queue of schedulable jobs
        self.pending_queue = list()

        # currently scheduled jobs
        self.current_job = None

        # default time-slice length in microseoncs
        self.TIME_SLICE = 10000

    def _timedeltas(self, series):
        # return the difference between every even and odd value in series
        series = np.reshape(series, (-1, 2))
        deltas = np.diff(series)
        return np.reshape(deltas, -1)

    def response_times(self):
        """Return dict with response times list for each thread"""
        response_times = dict()
        for th, series in self.latency_trace.items():
            # first value is arrival, second is start, third is completion
            #  -> mask start values
            mask = np.ones(len(series), dtype=bool)
            mask[np.arange(1, len(series), 3)] = False
            response_times[th] = self._timedeltas(np.array(series)[mask])

        return response_times

    def sched_latencies(self):
        """Return dict with scheduling latency list for each thread"""
        latencies = dict()
        for th, series in self.latency_trace.items():
            # first value is arrival, second is start, third is completion
            #  -> mask completion values
            mask = np.ones(len(series), dtype=bool)
            mask[np.arange(2, len(series), 3)] = False
            latencies[th] = self._timedeltas(np.array(series)[mask])

        return latencies

    def time_slices(self):
        """Return dict with time slice lengths for each thread"""

        slices = dict()

        # first extract list of time stamps for each thread
        for (thread, time, weight) in self.trace:
            if thread not in slices:
                slices[thread] = list()

            slices[thread].append(time)

        # calculate slices for each thread
        for th in slices:
            series = np.reshape(slices[th], (-1,2))
            slices[th] = np.reshape(np.diff(series), -1)

        return slices

    def _as_intervals(self, lst):
        it = iter(lst)
        return iter(lambda: tuple(islice(it, 2)), ())

    def cpu_shares(self):
        """Return fractional CPU share for every thread from first activation to last completion"""

        thread_times = {'idle' : list()}
        for (th, time, weight) in self.trace:
            if th not in thread_times:
                thread_times[th] = list()
            thread_times[th].append(time)

        # create pandas Intervals from idle times
        idle_intervals = list()
        for start, end in self._as_intervals(thread_times['idle']):
            idle_intervals.append(pd.Interval(start, end, closed='left'))

        # iterate threads
        shares = dict()
        for th, series in self.latency_trace.items():
            activity_interval = pd.Interval(series[0], series[-1], closed='left')
            total_time = activity_interval.length

            # sum up executions
            cpu_time = 0
            for start, end in self._as_intervals(thread_times[th]):
                cpu_time += end - start

            for idle in idle_intervals:
                if idle.overlaps(activity_interval):
                    total_time -= idle.length

            shares[th] = cpu_time / total_time

        return shares

    def start_job(self, j):
        """
        Start a job by adding it to the pending queue.

        Note: Ever thread must have at most one started job.

        Parameters
        ----------
            j : Job
                Job to be started
        """
        threads = [job.thread for job in self.pending_queue]
        if self.current_job is not None:
            threads.append(self.current_job.thread)

        if j.thread in threads:
            print("Unable to schedule thread which still has a job in queue: %s" % j)
        else:
            print("Thread%s started at %d" % (j.thread, j.arrival))

            # add arrival time to latency trace
            if j.thread not in self.latency_trace:
                self.latency_trace[j.thread] = list()
            self.latency_trace[j.thread].append(j.arrival)

            # insert job
            self.insert_job(j)

    def insert_job(self, j):
        """Insert new job into pending queue"""
        self.pending_queue.append(j)

    def finish_job(self, j):
        """
        Completes a job.

        Completion may release waiting jobs. The completed job may also restart
        itself after a certain time.

        Parameters
        ----------
            j : Job
                Job to be completed
        """
        print("Thread%s finished at %d" % (j.thread, j.finish_time))
        # add completion time to latency trace
        self.latency_trace[j.thread].append(j.finish_time)

        # schedule waiting jobs that are triggered by finish event
        if j.finish_event in self.waiting_jobs:
            for wj in self.waiting_jobs.pop(j.finish_event):
                self.start_job(wj.start(j.finish_time))

        # store new job if it restarts itself
        restart_job = j.restart(j.finish_time)
        if restart_job is not None:
            self.restarted_jobs.append(restart_job)
            self.restarted_jobs.sort(key=lambda x: x.arrival)

    def take_next_job(self):
        """Takes and returns the next Job from the reader"""
        job = self.next_job

        while True:
            self.next_job = self.reader.next()
            if isinstance(self.next_job, WaitingJob):
                e = self.next_job.start_event
                if e not in self.waiting_jobs:
                    self.waiting_jobs[e] = list()
                self.waiting_jobs[e].append(self.next_job)
            else:
                break

        return job

    def take_ready_jobs(self):
        """Releases all newly activated jobs"""
        while self.restarted_jobs and self.restarted_jobs[0].arrival <= self.time:
            self.start_job(self.restarted_jobs.pop(0))

        while self.next_job and self.next_job.arrival <= self.time:
            self.start_job(self.take_next_job())

    def update_job(self, j, executed):
        """
        Update a Job after it has been executed for a certain time.

        Parameters
        ----------
            j : Job
                Job object
            executed : int
                time interval for which j has been executed
        """

        # add start time to latency trace
        if j.executed_time <= 0 and not hasattr(j, "started"):
            j.started = True
            self.latency_trace[j.thread].append(self.last_time)

        j.executed_time += executed

        # trace execution
        self.trace.append([j.thread, self.last_time,            j.weight])
        self.trace.append([j.thread, self.last_time + executed, j.weight])

    def try_finish_job(self, j):
        """
        Updates a job after it has been scheduled and checks whether it completed.

        Paramters
        ---------
            j : Job

        Returns
        -------
        True if j has been completed.
        """
        executed = min(self.time - self.last_time, j.runtime_left())
        self.update_job(j, executed)

        if j.runtime_left() == 0:
            j.finish_time = self.last_time + executed
            self.finish_job(j)
            self.last_time = self.time
            return True

        self.last_time = self.time

        return False

    def idle(self):
        """Returns True if the next job arrives in the future"""
        return self.next_job and self.next_job.arrival > self.time

    def do_idle(self):
        """Advances time to the arrival of the next job"""
        self.trace.append(['idle', self.time,              1])
        self.trace.append(['idle', self.next_preemption(), 1])
        self.current_job = None
        self.time      = self.next_preemption()
        self.last_time = self.time

    def next_preemption(self):
        """Returns time of the next job arrival"""
        candidates = list()
        if self.next_job:
            candidates.append(self.next_job.arrival)

        if len(self.restarted_jobs):
            candidates.append(self.restarted_jobs[0].arrival)

        if len(candidates) == 0:
            return float('inf')

        return min(candidates)

    def time_until(self, t):
        if t == float('inf'):
            return t

        if t < self.time:
            raise Exception("Cannot compute time delta to a past timestamp.")

        return t - self.time

    def execute(self, reader):
        """Executes the jobs provided by the reader"""
        self.reader = reader
        self.take_next_job()

        if self.next_job:
            self.time = self.next_job.arrival

        while self.schedule():
            pass

    def time_slice(self, j):
        """Returns length of the time slice for Job j"""
        return self.TIME_SLICE

    def reinsert_job(self, j):
        """Inserts Job j into the pending queue"""
        self.pending_queue.append(j)

    def schedule(self):
        """Performs a scheduling decision"""
        current_job_finished = True
        if self.current_job != None:
            if not self.try_finish_job(self.current_job):
                current_job_finished = False

        print("[%07d] %s" % (self.time, self.current_job))

        if current_job_finished:
            self.current_job = None

        # check whether there is any restarted job ready
        self.take_ready_jobs()

        if self.current_job is not None:
            self.reinsert_job(self.current_job)

        if len(self.pending_queue):
            self.current_job = self.choose_job()
            self.time += min(self.time_slice(self.current_job), self.current_job.runtime_left())
        elif self.idle():
            self.do_idle()
        else:
            print("No more jobs")
            return False

        return True


class RoundRobin(Scheduler):
    """Round-Robin scheduler"""

    def choose_job(self):
        """Returns the Job at the head of the queue"""
        return self.pending_queue.pop(0)


class Stride(Scheduler):
    """Stride scheduler"""

    def __init__(self):
        Scheduler.__init__(self)

        # we need to store the last virtual time of each thread
        self.last_vtime = dict()

    def insert_job(self, j):
        # insert new jobs at the head of the queue according to their weight
        #  such that new jobs with higher weight are chosen before new jobs with
        #  lower weight before old jobs
        for i in range(len(self.pending_queue)):
            if hasattr(self.pending_queue[i], "started"):
                # insert just before any already started job
                self.pending_queue.insert(i, j)
                return

            # insert before any not-started job with lower or equal weight
            if j.weight >= self.pending_queue[i].weight:
                self.pending_queue.insert(i, j)
                return

        self.pending_queue.append(j)

    def avt(self, j):
        """Returns actual virtual time of a job"""
        return self.thread_vt(j.thread)

    def thread_vt(self, t, increment=0):
        """Returns the (incremented) virtual time of a thread."""
        if t not in self.last_vtime:
            self.last_vtime[t] = self.svt()

        if increment > 0:
            self.last_vtime[t] += increment

        return self.last_vtime[t]

    def svt(self):
        """Returns the scheduler virtual time"""
        if len(self.pending_queue) == 0 and self.current_job is None:
            return 0

        min_avt, dummy = self.min_avt()
        return min_avt

    def _min_vt(self, vt, skip_jobs=set()):
        """
        Finds the minimum virtual time of all pending jobs

        Returns
        -------
        [minimum virtual time, index of job with minimum virtual time]
        """
        vtimes = [vt(j) if j not in skip_jobs else float('inf') for j in self.pending_queue]
        if self.current_job is not None and self.current_job not in skip_jobs:
            vtimes.append(vt(self.current_job))

        if len(vtimes) == 0:
            return [None, None]

        return [np.min(vtimes), np.argmin(vtimes)]

    def min_avt(self):
        """
        Finds the minimum actual virtual time of all pending jobs

        Returns
        -------
        [minimum virtual time, index of job with minimum virtual time]
        """
        return self._min_vt(vt=lambda j: self.avt(j))

    def update_job(self, j, executed):
        Scheduler.update_job(self, j, executed)

        # update virtual time
        self.thread_vt(j.thread, math.ceil(executed / j.weight))

    def start_job(self, j):
        # initialise the thread's virtual time
        vt = self.thread_vt(j.thread)

        # a thread with lower virtual time than SVT must be adapted to SVT
        min_vt = self.svt()
        if min_vt > vt:
            self.thread_vt(j.thread, min_vt - vt)

        Scheduler.start_job(self, j)

    def choose_job(self):
        """Returns the job with the minimum virtual time"""
        dummy, job_index = self.min_avt()
        return self.pending_queue.pop(job_index)


class BVT(Stride):
    """BVT scheduler (Duda and Cheriton 1999)"""

    def __init__(self):
        Stride.__init__(self)

        # context-switch allowance (10ms)
        self.C = 10000

        self.second_best_job = None

    def choose_job(self):
        # return the job with minimum virtual time if we idled or there was only one job
        if self.current_job is None or self.second_best_job is None:
            dummy, job_index = self.min_vt()
            return self.pending_queue.pop(job_index)

        skip_jobs = [self.current_job, self.second_best_job]
        skip_jobs += [j for j in self.pending_queue if hasattr(j, 'started')]
        min_vt, job_index = self.min_vt(skip_jobs=set(skip_jobs))

        second_best_vt = self.thread_vt(self.second_best_job.thread)
        current_vt     = self.thread_vt(self.current_job.thread)

        # return job with minimum virtual time if another eligible job arrived in the meantime
        if min_vt > 0 and second_best_vt >= min_vt and current_vt >= min_vt:
            return self.pending_queue.pop(job_index)

        # second_best_vt is still the second best, see if we keep current job
        if (current_vt - second_best_vt)*self.current_job.weight < self.C:
            return self.pending_queue.pop(self.pending_queue.index(self.current_job))

        # return second best job
        return self.pending_queue.pop(self.pending_queue.index(self.second_best_job))

    def time_slice(self, j):
        # if j is the only job, run until preemption
        if len(self.pending_queue) == 0:
            return self.time_until(self.next_preemption())

        # find job with the second lowest virtual time
        next_vt, next_index = self.min_vt(skip_jobs=set([j]))
        min_vt = self.thread_vt(j.thread)

        self.second_best_job = self.pending_queue[next_index]

        # allow j to run for C ahead of the second best job or until the next preemption
        return min((next_vt - min_vt)*j.weight + self.C, self.time_until(self.next_preemption()))


class BaseHw(Scheduler):
    """
    Current quota-based scheduler implemented in base-hw

    Remarks
    -------
    - We interpret the weight as quota in %.
    - Jobs with priority 0 are handled as non-quoted jobs.
    """

    def __init__(self):
        Scheduler.__init__(self)
        self.SUPERPERIOD = 1000 * 1000 # 1s
        self.last_superperiod = 0

        # we must store the CPU quantum per thread
        self.quota = dict()

    def start_round(self):
        self.last_superperiod = self.time

        # recalculate quota
        self.quota = dict()
        for j in self.pending_queue:
            if j.priority > 0:
                self.quota[j.thread] = self.SUPERPERIOD / 100 * j.weight
            else:
                self.quota[j.thread] = 0

    def update_job(self, j, executed):
        Scheduler.update_job(self, j, executed)
        if self.quota[j.thread] > 0:
            if executed > self.quota[j.thread]:
                raise Exception("Quota overuse for %s" % j)

            self.quota[j.thread] -= executed
            if self.quota[j.thread] == 0:
                print("")
                print("Thread%s depleted its quota at %s" % (j.thread, self.time))
                j.one_time_boost = True

    def start_job(self, j):
        if j.thread not in self.quota:
            if j.priority > 0:
                self.quota[j.thread] = self.SUPERPERIOD / 100 * j.weight
            else:
                self.quota[j.thread] = 0

        Scheduler.start_job(self, j)

    def choose_job(self):
        if self.time >= self.last_superperiod + self.SUPERPERIOD:
            self.start_round()

        # build list of pending jobs' priorities lowering jobs with depleted
        #  quota to default priority
        job_prios = [j.priority if self.quota[j.thread] > 0 else 0 for j in self.pending_queue]

        # find and return first highest priority job
        index = np.argmax(job_prios)
        return self.pending_queue.pop(index)

    def reinsert_job(self, j):
        # Jobs that depleted their quota will be added to the head of the queue.
        if hasattr(j, 'one_time_boost') and j.one_time_boost:
            j.one_time_boost = False
            self.pending_queue.insert(0,j)
        else:
            self.pending_queue.append(j)

    def time_slice(self, j):
        time_left_in_superperiod = self.last_superperiod + self.SUPERPERIOD - self.time
        if self.quota[j.thread] > 0:
            return min(self.TIME_SLICE, self.time_until(self.next_preemption()), self.quota[j.thread], time_left_in_superperiod)
        else:
            return min(self.TIME_SLICE, self.time_until(self.next_preemption()), time_left_in_superperiod)
