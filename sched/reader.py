import os
import csv
import pandas as pd
from .scheduler import Job, WaitingJob

class WorkloadReader(object):

    def __init__(self, filename):
        self.joblist = list()
        with open(filename) as f:
            reader = csv.DictReader(self.decomment(f), delimiter='\t')
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

                prio = 0
                if 'priority' in row and row['priority']:
                    prio = int(row['priority'])

                period = 0
                start = row['start']
                obj   = Job
                if start.startswith("every"):
                    period_td = pd.Timedelta(start.split()[1])
                    period    = period_td.seconds * 1000 * 1000 + period_td.microseconds
                    start     = 0
                elif start[0].isdigit():
                    start_td = pd.Timedelta(row['start'])
                    start    = start_td.seconds * 1000 * 1000 + start_td.microseconds
                else:
                    obj = WaitingJob

                self.joblist.append(obj(row['thread'],
                                        start,
                                        runtime_us,
                                        period=period,
                                        restart=restart_us,
                                        weight=float(row['weight']),
                                        prio=prio,
                                        event=event))

    def decomment(self, csvfile):
        for row in csvfile:
            raw = row.split('#')[0].strip()
            if raw:
                yield raw

    def empty(self):
        return len(self.joblist) == 0

    def next(self):
        if self.empty():
            return None
        return self.joblist.pop(0)
