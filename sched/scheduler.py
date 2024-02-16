
class Job(object):

    def __init__(self, thread, arrival, execution_time, weight=1, event=None):
        self.arrival        = int(arrival)
        self.execution_time = int(execution_time)
        self.thread         = thread
        self.executed_time  = 0
        self.weight         = weight
        self.finish_event   = event

    def __repr__(self):
        return "%d-Thread%s(%f) %d/%d" % (self.arrival, self.thread, self.weight, self.executed_time, self.execution_time)


class WaitingJob(object):

    def __init__(self, thread, start_event, execution_time, weight=1, event=None):
        self.start_event    = start_event
        self.execution_time = int(execution_time)
        self.thread         = thread
        self.weight         = weight
        self.finish_event   = event

    def start(self, time):
        return Job(self.thread, time, self.execution_time, self.weight, self.finish_event)


class Scheduler(object):

    def __init__(self):
        self.time = 0
        self.last_time = 0
        self.reader   = None
        self.next_job = None
        self.waiting_jobs = dict()
        self.response_times = dict()
        self.trace = list()

    def schedule_job(self, j):
        print("Thread%s started at %d" % (j.thread, j.arrival))

    def finish_job(self, j):
        print("Thread%s finished at %d" % (j.thread, j.finish_time))
        # calculate and save response time
        if j.thread not in self.response_times:
            self.response_times[j.thread] = list()
        self.response_times[j.thread].append(j.finish_time - j.arrival)

        if j.finish_event in self.waiting_jobs:
            for wj in self.waiting_jobs.pop(j.finish_event):
                self.schedule_job(wj.start(j.finish_time))

    def trace_execution(self, j, executed):
        self.trace.append([j.thread, self.last_time,            j.weight])
        self.trace.append([j.thread, self.last_time + executed, j.weight])

    def tick(self, delta):
        self.time += delta

    def take_next_job(self):
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
        while self.next_job and self.next_job.arrival <= self.time:
            self.schedule_job(self.take_next_job())

    def unschedule_job(self, j):
        executed = min(self.time - self.last_time, j.execution_time - j.executed_time)
        j.executed_time += executed
        self.trace_execution(j, executed)
        if j.executed_time == j.execution_time:
            j.finish_time = self.last_time + executed
            self.finish_job(j)
            self.last_time = self.time
            return True

        self.last_time = self.time

        return False

    def idle(self):
        return self.next_job and self.next_job.arrival > self.time

    def do_idle(self):
        self.current_job = None
        self.time      = self.next_job.arrival
        self.last_time = self.time

    def execute(self, reader):
        self.reader = reader
        self.take_next_job()

        if self.next_job:
            self.time = self.next_job.arrival

        while self.schedule_any():
            pass

class RoundRobin(Scheduler):

    def __init__(self):
        Scheduler.__init__(self)
        self.pending_queue = list()
        self.current_job = None
        self.TIME_SLICE = 10000

    def schedule_job(self, j):
        threads = [job.thread for job in self.pending_queue]
        if self.current_job is not None:
            threads.append(self.current_job.thread)

        if j.thread in threads:
            print("Unable to schedule thread which still has a job in queue: %s" % j)
        else:
            Scheduler.schedule_job(self, j)
            self.pending_queue.append(j)

    def schedule_any(self):
        self.take_ready_jobs()

        if self.current_job != None:
            if not self.unschedule_job(self.current_job):
                self.pending_queue.append(self.current_job)

        print("[%07d] %s" % (self.time, self.current_job))

        if len(self.pending_queue):
            self.current_job = self.pending_queue.pop(0)
            self.tick(self.TIME_SLICE)
        elif self.idle():
            self.do_idle()
        else:
            print("No more jobs")
            return False

        return True
