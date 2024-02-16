import os
import csv
import pandas as pd
from .scheduler import Job

class WorkloadReader(object):

    def __init__(self, filename):
        self.joblist = list()
        with open(filename) as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                start   = pd.Timedelta(row['start'])
                runtime = pd.Timedelta(row['runtime'])
                start_us   = start.seconds * 1000 * 1000 + start.microseconds
                runtime_us = runtime.seconds * 1000 * 1000 + runtime.microseconds
                self.joblist.append(Job(row['thread'],
                                        start_us,
                                        runtime_us,
                                        float(row['weight'])))

    def empty(self):
        return len(self.joblist) == 0

    def next(self):
        if self.empty():
            return None
        return self.joblist.pop(0)
