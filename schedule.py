#!/usr/bin/env python

import os
import argparse
from sched import reader
from sched import scheduler
from plot  import plot

parser = argparse.ArgumentParser(description="Simulate CPU scheduler")
parser.add_argument("filename", type=str)
parser.add_argument("--policy", choices=["rr", "stride", "base-hw", "bvt"], default="rr")
parser.add_argument("--plot_latencies", action="store_true")

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

if args.plot_latencies:
    # plot response time distribution
    plot.LatencyPlot(s.response_times).show()

# plot virtual time and fairness
plot.FairnessPlot(s.trace).show()
