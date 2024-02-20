import numpy as np

class Job(object):
    """
    A standard job.

    After a job arrival time has passed it is always schedulable. A job has a
    weight and a priority. After a job has executed for its specified execution
    time, it may release a finish event (to trigger other jobs) or restart
    itself after a certain time interval.
    """

    def __init__(self, thread, arrival, execution_time, restart=0, weight=1, prio=0, event=None):
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
        self.thread         = thread
        self.executed_time  = 0
        self.weight         = int(weight)
        self.finish_event   = event
        self.priority       = int(prio)

    def restart(self, completion_time):
        """
        Creates a new Job if this Job is restarted after completion. The new
        Job's arrival time is set to completion time + restart time.

        Parameters
        ----------
            completion_time: int
                completion time of this job

        Returns
        -------
            None or Job object
        """
        if self.restart_time == 0:
            return None
        else:
            return Job(self.thread, completion_time + self.restart_time,
                       self.executed_time, self.restart_time, self.weight,
                       self.priority, self.finish_event)

    def __repr__(self):
        return "%d-Thread%s(%f) %d/%d" % (self.arrival, self.thread, self.weight, self.executed_time, self.execution_time)


class WaitingJob(object):
    """ A waiting job is activated by another job's completion event."""

    def __init__(self, thread, start_event, execution_time, restart=0, weight=1, prio=0, event=None):
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
        self.thread         = thread
        self.weight         = int(weight)
        self.finish_event   = event
        self.priority       = int(prio)

    def start(self, time):
        """Start this job at the given time and return the corresponding Job object."""
        return Job(self.thread, time, self.execution_time, self.restart_time, self.weight, self.finish_event,
                   self.priority)


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

        # dictionary of resulting response times indexed by thread name
        self.response_times = dict()

        # resulting execution trace
        self.trace = list()

        # list/queue of schedulable jobs
        self.pending_queue = list()

        # currently scheduled jobs
        self.current_job = None

        # default time-slice length in microseoncs
        self.TIME_SLICE = 10000

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
        # calculate and save response time
        if j.thread not in self.response_times:
            self.response_times[j.thread] = list()
        self.response_times[j.thread].append(j.finish_time - j.arrival)

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
        j.executed_time += executed
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
        executed = min(self.time - self.last_time, j.execution_time - j.executed_time)
        self.update_job(j, executed)

        if j.executed_time == j.execution_time:
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
        self.current_job = None
        self.time      = self.next_job.arrival
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
        self.take_ready_jobs()

        if self.current_job != None:
            if not self.try_finish_job(self.current_job):
                self.reinsert_job(self.current_job)

        print("[%07d] %s" % (self.time, self.current_job))

        if len(self.pending_queue):
            self.current_job = self.choose_job()
            self.time += self.time_slice(self.current_job)
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

    def min_vt(self):
        """
        Finds the minimum virtual time of all pending jobs

        Returns
        -------
        [minimum virtual time, index of job with minimum virtual time]
        """
        vtimes = [j.virtual_time for j in self.pending_queue]
        if len(vtimes) == 0:
            return [0, 0]

        return [np.min(vtimes), np.argmin(vtimes)]

    def update_job(self, j, executed):
        Scheduler.update_job(self, j, executed)

        # update virtual time
        j.virtual_time += executed / j.weight

    def start_job(self, j):
        # set virtual time
        j.virtual_time, dummy = self.min_vt()

        Scheduler.start_job(self, j)

    def choose_job(self):
        """Returns the job with the minimum virtual time"""
        dummy, job_index = self.min_vt()
        return self.pending_queue.pop(job_index)


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
            return min(self.TIME_SLICE, self.next_preemption(), self.quota[j.thread], time_left_in_superperiod)
        else:
            return min(self.TIME_SLICE, self.next_preemption(), time_left_in_superperiod)
