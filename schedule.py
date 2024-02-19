#!/usr/bin/env python

import os
import argparse
from sched import reader
from sched import scheduler
from plot  import plot

parser = argparse.ArgumentParser(description="Simulate CPU scheduler")
parser.add_argument("filename", type=str)
parser.add_argument("--policy", choices=["rr", "stride"], default="rr")
parser.add_argument("--plot_latencies", action="store_true")

args = parser.parse_args()

trace = reader.WorkloadReader(args.filename)

if args.policy == "rr":
    s = scheduler.RoundRobin()
elif args.policy == "stride":
    s = scheduler.Stride()

s.execute(trace)

if args.plot_latencies:
    # plot response time distribution
    plot.LatencyPlot(s.response_times).show()

# plot virtual time and fairness
plot.FairnessPlot(s.trace).show()
