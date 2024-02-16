#!/usr/bin/env python

import os
import argparse
from sched import reader
from sched import scheduler
from plot  import plot

parser = argparse.ArgumentParser(description="Simulate CPU scheduler")
parser.add_argument("filename",    type=str)

args = parser.parse_args()

trace = reader.WorkloadReader(args.filename)

s = scheduler.RoundRobin()
s.execute(trace)

# plot response time distribution
plot.LatencyPlot(s.response_times).show()

# plot virtual time and fairness
plot.FairnessPlot(s.trace).show()
