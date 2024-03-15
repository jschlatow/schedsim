#!/usr/bin/env python

import os
import argparse
import numpy as np
from sched import reader
from sched import scheduler
from sched import bvt_standalone
from plot  import plot

parser = argparse.ArgumentParser(description="Simulate CPU scheduler")
parser.add_argument("filename", type=str)
parser.add_argument("--policy", choices=["rr", "stride", "base-hw", "sa-bvt", "bvt", "bvt-nowarp", "bvt-sculpt", "sa-bvt-sculpt"], default="rr")
parser.add_argument("--plot_latencies", action="store_true")
parser.add_argument("--plot_trace",     action="store_true")
parser.add_argument("--plot_slices",    action="store_true")
parser.add_argument("--simulation_time", type=int, default=10*1000*1000, help="Maximum simulation time in microseconds")

args = parser.parse_args()

trace = reader.WorkloadReader(args.filename)

if args.policy == "rr":
    s = scheduler.RoundRobin()
elif args.policy == "stride":
    s = scheduler.Stride()
elif args.policy == "base-hw":
    s = scheduler.BaseHw()
elif args.policy == "bvt":
    s = scheduler.BVT(warp=True)
elif args.policy == "sa-bvt":
    s = bvt_standalone.BVT(warp=True)
elif args.policy == "bvt-nowarp":
    s = scheduler.BVT(warp=False)
elif args.policy == "bvt-sculpt":
    s = scheduler.BVTSculpt()
elif args.policy == "sa-bvt-sculpt":
    s = bvt_standalone.BVTSculpt()

s.execute(trace, max_time=args.simulation_time)

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
time_slices = s.time_slices()
for th in sorted(response_times.keys()):
    print("\tThread%s: %d" % (th, len(time_slices[th])-1))

print()
print("CPU shares:")
shares = s.cpu_shares()
for th in sorted(shares.keys()):
    print("\tThread%s: %f" % (th, shares[th]))

if args.plot_latencies:
    # plot response time distribution
    plot.LatencyPlot(response_times,  "Response time", sched_latencies, "Scheduling latency").show()

if args.plot_trace:
    plot.TracePlot(s.latency_trace).show()

if args.plot_slices:
    plot.HistPlot(time_slices, "Time slice length").show()

# plot virtual time and fairness
plot.FairnessPlot(s.trace).show()
