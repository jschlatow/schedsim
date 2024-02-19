import os
import csv
import pandas as pd
from .scheduler import Job, WaitingJob

class WorkloadReader(object):

    def __init__(self, filename):
        self.joblist = list()
        with open(filename) as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                runtime = pd.Timedelta(row['runtime'])
                runtime_us = runtime.seconds * 1000 * 1000 + runtime.microseconds

                event = None
                if 'event' in row:
                    event = row['event']

                restart_us = 0
                if 'restart' in row and row['restart']:
                    restart = pd.Timedelta(row['restart'])
                    restart_us = restart.seconds * 1000 * 1000 + restart.microseconds

                if row['start'][0].isdigit():
                    start   = pd.Timedelta(row['start'])
                    start_us   = start.seconds * 1000 * 1000 + start.microseconds

                    self.joblist.append(Job(row['thread'],
                                            start_us,
                                            runtime_us,
                                            restart_us,
                                            float(row['weight']),
                                            event))
                else:
                    self.joblist.append(WaitingJob(row['thread'],
                                            row['start'],
                                            runtime_us,
                                            restart_us,
                                            float(row['weight']),
                                            event))


    def empty(self):
        return len(self.joblist) == 0

    def next(self):
        if self.empty():
            return None
        return self.joblist.pop(0)
