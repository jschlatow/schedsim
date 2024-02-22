#!/usr/bin/env python

import os
import argparse
import numpy as np
from sched import reader
from sched import scheduler
from plot  import plot

parser = argparse.ArgumentParser(description="Simulate CPU scheduler")
parser.add_argument("filename", type=str)
parser.add_argument("--policy", choices=["rr", "stride", "base-hw", "bvt"], default="rr")
parser.add_argument("--plot_latencies", action="store_true")
parser.add_argument("--plot_trace", action="store_true")

args = parser.parse_args()

trace = reader.WorkloadReader(args.filename)

if args.policy == "rr":
    s = scheduler.RoundRobin()
elif args.policy == "stride":
    s = scheduler.Stride()
elif args.policy == "base-hw":
    s = scheduler.BaseHw()
elif args.policy == "bvt":
    s = scheduler.BVT()

s.execute(trace)

# print stats
print()
print("Response times:")
response_times = s.response_times()
for th in sorted(response_times.keys()):
    values = response_times[th]
    print("Thread%s (%d) \tmin: %d\tmean: %d\tmedian: %d\tmax: %d" % (th, len(values), min(values), np.mean(values), np.median(values), max(values)))

print()
print("Scheduling latencies:")
sched_latencies = s.sched_latencies()
for th in sorted(sched_latencies.keys()):
    values = sched_latencies[th]
    extra = ""
    if min(values) == 0:
        extra = "\t"
    print("Thread%s (%d) \tmin: %d%s\tmean: %d\tmedian: %d\tmax: %d" % (th, len(values), min(values), extra, np.mean(values), np.median(values), max(values)))

print()
print("Context switches:")
for th in sorted(response_times.keys()):
    trace_times = [ts for (t, ts, w) in s.trace if t == th]
    print("\tThread%s: %d" % (th, len(trace_times)/2))

if args.plot_latencies:
    # plot response time distribution
    plot.LatencyPlot(response_times,  "Response time", sched_latencies, "Scheduling latency").show()

if args.plot_trace:
    plot.TracePlot(s.latency_trace).show()

# plot virtual time and fairness
plot.FairnessPlot(s.trace).show()
