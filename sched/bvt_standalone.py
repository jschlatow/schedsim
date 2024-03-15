import math
import functools

from sortedcontainers import SortedList

from .scheduler import Scheduler
from .scheduler import Job
from .scheduler import WaitingJob

class JobTree(object):
    """Data structure holding a binary tree of jobs (i.e. active threads)
       sorted by effective virtual time (EVT)
    """

    def __init__(self, lookup_avt, lookup_evt):
        # remark: I'm using lookup functions for AVT and EVT here because the
        #  generic scheduler only has a notion of jobs and not threads. In a
        #  real implementation, AVT should be an attribute of a thread object
        #  and EVT is calculated based on warp value and warp status.
        self.lookup_avt       = lookup_avt
        self.lookup_evt       = lookup_evt
        self.tree             = SortedList()
        self.cached_svt       = 0
        self.svt_needs_update = False

    def insert(self, job):
        # remark: This is a hack to make jobs comparable.
        job.compare_func = lambda rhs: self.lookup_evt(job) <= self.lookup_evt(rhs)

        if len(self.tree) == 0:
            self.cached_svt = self.lookup_avt(job)
            self.svt_needs_update = False

        self.tree.add(job)

    def min(self):
        """Returns minimum key (i.e. EVT)"""
        return self.tree[0]

    def pop_min(self):
        """Returns EVT and Job with minimum EVT"""
        j = self.tree.pop(0)

        # mark SVT as needed to be updated when we take a job with AVT == SVT
        if self.lookup_avt(j) == self.cached_svt:
            self.svt_needs_update = True

        return j

    def find_svt(self, current_job):
        """Find minimum actual virtual time (AVT) in tree"""
        min_avt = float('inf')
        if current_job is not None:
            min_avt = self.lookup_avt(current_job)

        # traverse in order (ordered by EVT)
        for j in self.tree:
            min_avt = min(self.lookup_avt(j), min_avt)

            # we can skip the search once we found a non-warping job
            if not hasattr(j, "warped") or not j.warped:
                break

        # keep SVT if tree is empty
        if min_avt == float('inf'):
            return self.cached_svt

        return min_avt

    def svt(self, current_job):
        """Returns scheduler virtual time (SVT)"""
        if self.svt_needs_update:
            self.cached_svt = self.find_svt(current_job)
            self.svt_needs_update = False

        return self.cached_svt

    def __len__(self):
        """Required for compatibility with generic Scheduler"""
        return len(self.tree)

    def __iter__(self):
        """Required for compatibility with generic Scheduler"""
        return iter(self.tree)


class BVT(Scheduler):
    """Standalone BVT scheduler"""

    def __init__(self, warp=True, scheduler=None):
        Scheduler.__init__(self)

        self.main_scheduler = self
        if scheduler is not None:
            self.main_scheduler = scheduler

        # replace pending queue with binary trees
        self.pending_queue   = JobTree(lambda j: self.thread_avt[j.thread],
                                       lambda j: self.evt(j))

        # context-switch allowance (10ms)
        self.C = 10000

        # warp value, warp-time limit and unwarp time requirement for priorities 0 to 3
        if warp:
            self.warp        = [0,   5000, 1000000, 2000000]
            self.warp_limit  = [0,  50000,   20000,   20000]
        else:
            self.warp_limit  = [0,      0,       0,       0]
            self.warp        = [0,      0,       0,       0]

        # we need to store the last virtual time of each thread
        #  (can be stored as a thread attribute)
        self.thread_avt = dict()

    def update_job(self, j, executed):
        # save warped execution time
        if hasattr(j, "warped") and j.warped:
            j.warp_time += executed

        Scheduler.update_job(self.main_scheduler, j, executed)

        # update virtual time
        self.thread_avt[j.thread] += math.ceil(executed / j.weight)

    def start_job(self, j, warp=None, warp_limit=None, **kwargs):
        """Start job j and warp if allowed on j's priority"""

        # a thread with lower virtual time than SVT must be adapted to SVT
        svt = self.pending_queue.svt(self.current_job)
        if j.thread not in self.thread_avt or self.thread_avt[j.thread] < svt:
            self.thread_avt[j.thread] = svt

        if warp is None:
            warp       = self.warp[j.priority]
        if warp_limit is None:
            warp_limit = self.warp_limit[j.priority]

        if warp > 0:
            j.warped     = True
            j.warp_limit = warp_limit
            j.warp       = warp
            j.warp_time  = 0

        # Scheduler.start_job() calls insert_job(), hence the job must have a valid AVT and warp value
        Scheduler.start_job(self.main_scheduler, j)

    def finish_job(self, j):
        """Unwarp job j before finishing"""
        if hasattr(j, "warped") and j.warped:
            self.unwarp(j)
        Scheduler.finish_job(self.main_scheduler, j)

    def warp_time_left(self, j):
        """Return the time when job j is forced to unwarp"""
        if not hasattr(j, "warped") or not j.warped or j.warp_limit == 0:
            return float('inf')

        if j.warp_time < j.warp_limit:
            return j.warp_limit - j.warp_time

        return 0

    def unwarp(self, j):
        j.warped = False
        j.warp = 0

    def warp_value(self, j):
        """Returns warp value for job j"""
        if not hasattr(j, "warped") or not j.warped:
            return 0
        elif self.warp_time_left(j) == 0:
            self.unwarp(j)

        return j.warp

    def evt(self, j):
        avt  = self.thread_avt[j.thread]
        warp = self.warp_value(j)
        evt  = avt
        if warp:
            evt = avt - warp

        return evt

    def insert_job(self, j):
        self.pending_queue.insert(j)

    def reinsert_job(self, j):
        pass

    def reinsert_job_before(self, j):
        self.insert_job(j)

    def choose_job(self):
        # We always choose the job with minimum EVT. In principle, we could
        # check whether the current job could still run because it is not ahead
        # of the second best job. However, this complicates the implementation
        # without any significant benefits.
        if len(self.pending_queue) == 0:
            raise Exception("tree is empty")
        return self.pending_queue.pop_min()

    def time_slice(self, j):
        # if j is the only job, run until preemption
        if len(self.pending_queue) == 0:
            return self.time_until(self.main_scheduler.next_preemption())

        # find job with the second lowest effective virtual time
        next_evt    = self.evt(self.pending_queue.min())
        current_evt = self.evt(self.current_job)

        if next_evt < current_evt:
            raise Exception("We accidentally picked a job that has not the smalles EVT.")

        # allow j to run for C ahead of the second best job,
        # until the next preemption or until its warp limit
        return min((next_evt - current_evt)*j.weight + self.C,
                    self.time_until(self.main_scheduler.next_preemption()),
                    self.warp_time_left(j))


@functools.total_ordering
class VirtualJob(object):

    def __init__(self, second_level_scheduler, name, weight):
        self.second_level_scheduler = second_level_scheduler
        self.executed_time = 0
        self.thread        = name
        self.weight        = weight
        self.arrival       = 0

    def __repr__(self):
        return self.thread

    def __le__(self, rhs):
        if hasattr(self, "compare_func"):
            return self.compare_func(rhs)

        return NotImplemented

    def __eq__(self, rhs):
        return self.thread == rhs.thread


class BVTTopLevel(BVT):
    """BVT scheduler for top-level scheduler"""

    def choose_job(self):
        # we must sort the job tree because warp values might have changed
        self.pending_queue.tree = SortedList(self.pending_queue.tree)

        return BVT.choose_job(self)


class BVTSculpt(Scheduler):
    """Standalone hierarchical BVT scheduler"""

    def __init__(self):
        Scheduler.__init__(self)

        self.priority_to_class = [0, 1, 2, 2]

        self.second_level_schedulers = [BVT(scheduler=self), BVT(scheduler=self), BVT(scheduler=self)]
        self.vjobs = [VirtualJob(self.second_level_schedulers[0], 'VBackground',  1),
                      VirtualJob(self.second_level_schedulers[1], 'VDesktop',     6),
                      VirtualJob(self.second_level_schedulers[2], 'VLowLatency', 20)]

        self.first_level_scheduler = BVTTopLevel()

        self.warp        = [0,   5000, 15000, 30000]
        self.warp_limit  = [0,  50000, 20000, 20000]

        self.pending_queue = self

        # context-switch allowance (10ms)
        self.C = 10000

    def __len__(self):
        """Required for compatibility with generic Scheduler"""
        result = 0
        for s in self.second_level_schedulers:
            result += len(s.pending_queue)

        return result

    def __iter__(self):
        """Required for compatibility with generic Scheduler"""
        for s in self.second_level_schedulers:
            for j in s.pending_queue:
                yield j

    def update_job(self, j, executed):
        c = self.priority_to_class[j.priority]

        # pass to second-level scheduler
        s = self.second_level_schedulers[c]
        s.update_job(j, executed)

        # update virtual execution time of virtual job
        self.first_level_scheduler.update_job(self.vjobs[c], executed)

    def start_job(self, j, signalling_job=None):
        c = self.priority_to_class[j.priority]
        s = self.second_level_schedulers[c]

        # handle warp boosting due to signalling
        warp       = self.warp[j.priority]
        warp_limit = self.warp_limit[j.priority]
        if signalling_job is not None and hasattr(signalling_job, "warped"):
            ss = self.second_level_schedulers[self.priority_to_class[signalling_job.priority]]
            warp = max(self.warp[signalling_job.priority], warp)
            if warp_limit == 0:
                warp_limit = ss.warp_time_left(signalling_job)
            else:
                warp_limit = min(ss.warp_time_left(signalling_job), warp_limit)

        # insert into second-level scheduler
        s.start_job(j, warp=warp, warp_limit=warp_limit)

        # insert virtual job into first-level scheduler
        if len(s.pending_queue) == 1:
            self.first_level_scheduler.start_job(self.vjobs[c], warp=warp, warp_limit=0)

    def finish_job(self, j):
        c = self.priority_to_class[j.priority]
        s = self.second_level_schedulers[c]

        s.finish_job(j)

    def insert_job(self, j):
        c = self.priority_to_class[j.priority]
        s = self.second_level_schedulers[c]

        s.insert_job(j)

    def reinsert_job_before(self, j):
        """Reinsert current second-level job before inserting new jobs"""
        # insert into second-level scheduler
        self.insert_job(j)

    def reinsert_job(self, j):
        pass

    def choose_job(self):
        # reinsert first-level job before choosing the next job
        vj = self.first_level_scheduler.current_job
        if vj is not None and len(vj.second_level_scheduler.pending_queue) > 0:
            self.first_level_scheduler.insert_job(vj)
            self.first_level_scheduler.current_job = None

        # determine second-level warp values and propagate to virtual jobs
        for vj in self.first_level_scheduler.pending_queue:
            j    = vj.second_level_scheduler.pending_queue.min()
            warp = vj.second_level_scheduler.warp_value(j)
            if warp > 0:
                vj.warped     = True
                vj.warp       = warp
                vj.warp_limit = 0
            else:
                vj.warped = False
                vj.warp   = 0

        # choose first-level and second-level jobs
        self.first_level_scheduler.current_job = self.first_level_scheduler.choose_job()
        s = self.first_level_scheduler.current_job.second_level_scheduler
        s.current_job = s.choose_job()

        return s.current_job

    def time_slice(self, j):
        # if j is the only job, run until preemption
        if len(self) == 0:
            return self.time_until(self.next_preemption())

        # determine first-level time slice
        first_level_slice = self.first_level_scheduler.time_slice(self.first_level_scheduler.current_job)

        c = self.priority_to_class[j.priority]
        s = self.second_level_schedulers[c]

        return min(s.time_slice(j), first_level_slice)
