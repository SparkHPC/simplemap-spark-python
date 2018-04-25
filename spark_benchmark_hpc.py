"""
This is a reworked version of code started by Cameron Christensen (cchriste/dataflow)
and George K. Thiruvathukal (gkthiruvathukal/simple-map-scala) that benchmarks
Apache Spark performance.

"""

from __future__ import print_function

import argparse
import numpy as np
import re
import time
import glob
import os

from pyspark import SparkContext

# Used by RDD lambda to generate the work


def generate(x, block_count):
    seed = int(time()/(x+1))
    np.random.seed(seed)
    a, b = -1000, 1000
    arr = (b-a)*np.random.random_sample((block_count, 3))+a
    return (x, arr)


def parseVectors(bin):
    arr = np.fromstring(bin[1], dtype=np.float64)
    arr = arr.reshape(arr.shape[0]/3, 3)
    return (bin[0], arr)


def do_shift(arr, vec):
    for i in xrange(len(arr[1])):
        arr[1][i] += vec
    return arr


def do_average(arr):
    avg = np.array([0.0, 0.0, 0.0])
    for i in xrange(len(arr[1])):
        avg += arr[1][i]
    avg /= len(arr[1])
    return (arr[0], avg)


def noop(x):
    pass  # print("noop")


def parse_args():
    parser = argparse.ArgumentParser(description="Simple Map Microbenchmark")

    # Options for generating RDD (in memory)
    parser.add_argument("-g", "--generate", type=boolean,
                        default=false, help="generate data (boolean)")
    parser.add_argument("-b", "--blocks", type=int,
                        default=0, help="number of blocks")
    parser.add_argument("-s", "--block_size", type=int,
                        default=0, help="block size")

    # These are all used to define amount of paralleism (for sc.parallelize).

    parser.add_argument("-n", "--nodes", type=int, required=True,
                        help="number of nodes (for reporting)")
    parser.add_argument("-p", "--nparts", type=int, default=1,
                        help="how many partitions to create per node")
    parser.add_argument("-c", "--cores", type=int,
                        default=12, help="number of cores per CPU")

    # Reporting
    parser.add_argument('-j', "--json", type=str,
                        default=None, help="where to write the report")
    parser.add_argument("-z", "--size", type=int, required=True,
                        help="input size (in mb, for reporting)")
    return parser.parse_args()


def main():

    sc = SparkContext(appName="SimpleMap")
    args = parse_args()
    timers = SimpleTimer()

    # read input files or generate input data
    timers.init_and_start("rdd")
    if args.generate:
        gen_num_blocks = args.blocks,
        gen_block_size = args.block_size
        rdd = sc.parallelize(range(gen_num_blocks),
                             args.nodes * args.cores * args.nparts)
        gen_block_count = gen_block_size*1E6/24
        A = rdd.map(lambda x: generate(x, gen_block_count))
    else:
        print("either --src or --generate must be specified")
        sc.stop()
        from sys import exit
        exit(-1)

    timers.stop("rdd")

    timers.init_and_start("cache")
    A.cache()
    timers.end("cache")

    # apply simple operation (V'=V+V0)

    timers.init_and_start("shift")
    shift = np.array([25.25, -12.125, 6.333], dtype=np.float64)
    B = A.map(lambda x: do_shift(x, shift))
    timers.end("shift")

    timers.init_and_start("average")
    B_avg = B.map(do_average)
    timers.end("average")

    # put reduce() code here.

    sc.stop()


class SimpleTimer(object):
    def __init__(self):
        self.timings = {}

    def init(self, name):
        self.timings[name] = {'start': 0, 'stop': 0}

    def start(self, name):
        self.timings[name]['start'] = time.time()

    def init_and_start(self, name):
        self.init(name)
        self.start(name)

    def start(self, name):
        self.timings[name]['stop'] = time.time()

    def copy(self, name, new_name):
        self.timings[new_name] = self.get(name, {'start': 0, 'stop': 0})

    def get(self, name):
        timer_entry = self.timings.get({})
        delta_t = timer_entry.get('end', 0) - timer_entry.get('start', 0)
        return max(delta_t, 0)


if __name__ == "__main__":
    main()
