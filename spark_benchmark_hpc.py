"""
This is a reworked version of code started by Cameron Christensen (cchriste/dataflow)
and George K. Thiruvathukal (gkthiruvathukal/simple-map-scala) that benchmarks
Apache Spark performance.

"""

#from __future__ import print_function

import argparse
import glob
import json
import numpy as np
import os
import re
import sys
import time

from pyspark import SparkContext, SparkConf

# Used by RDD lambda to generate the work


def generate(n, block_count):
    seed = int(time()/(n+1))
    np.random.seed(seed)
    a, b = -1000, 1000
    array = (b-a)*np.random.random_sample((block_count, 3))+a
    #return (n, arr)
    return array


# Only needed for file I/O, which we're not doing at the moment.
# Bring back later.

def parseVectors(bin):
    array = np.fromstring(bin[1], dtype=np.float64)
    array = arr.reshape(arr.shape[0]/3, 3)
    return array


def do_shift(array, vector_displacement):
    for i in xrange(len(arr[1])):
        array[1][i] += vector_displacement
    return array


def do_average(array):
    avg = np.array([0.0, 0.0, 0.0])
    for i in xrange(len(arr[1])):
        avg += arr[1][i]
    avg /= len(arr[1])
    return avg


def noop(x):
    pass  # print("noop")


def parse_args():
    parser = argparse.ArgumentParser(description="Simple Map Spark Microbenchmark - Python Version")

    # Options for generating RDD (in memory)
    parser.add_argument("-g", "--generate", action="store_true", default=False, help="generate data (boolean)")
    parser.add_argument("-b", "--blocks", type=int, default=0, help="number of blocks")
    parser.add_argument("-s", "--block_size", type=int, default=0, help="block size")

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
    #parser.add_argument("-z", "--size", type=int, required=True,
    #                    help="input size (in mb, for reporting)")
    return parser.parse_args()


def main():


    sconf = SparkConf()
    sc = SparkContext(appName="simplemap-spark-python", conf=sconf)
    python_version = sys.version
    spark_python_version = sc.pythonVer
    print("Python %s, Spark %s" % (python_version, spark_python_version))
    args = parse_args()
    timers = SimpleTimer()

    # read input files or generate input data
    timers.init_and_start("rdd")
    if args.generate:
        gen_num_blocks = args.blocks
        gen_block_size = args.block_size
        print("gen_num_blocks",gen_num_blocks)
        x = range(0, gen_num_blocks)
        rdd = sc.parallelize(range(0, gen_num_blocks), args.nodes * args.cores * args.nparts)
        gen_block_count = gen_block_size*1E6/24
        A = rdd.map(lambda n: generate(n, gen_block_count))
    else:
        print("either --generate must be specified")
        sc.stop()
        sys.exit(-1)

    timers.stop("rdd")

    timers.init_and_start("cache")
    A.cache()
    timers.stop("cache")

    # apply simple operation (V'=V+V0)

    timers.init_and_start("shift")
    shift = np.array([25.25, -12.125, 6.333], dtype=np.float64)
    B = A.map(lambda x: do_shift(x, shift))
    timers.stop("shift")

    timers.init_and_start("average")
    B_avg = B.map(do_average)
    timers.stop("average")

    if args.json:
      with open(args.json, "w") as report:
        json.dump(timers.get_all(), report)

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

    def stop(self, name):
        self.timings[name]['stop'] = time.time()

    def copy(self, name, new_name):
        self.timings[new_name] = self.get(name, {'start': 0, 'stop': 0})

    def get_name(self, name):
        timer_entry = self.timings.get(name, {})
        delta_t = timer_entry.get('stop', 0) - timer_entry.get('start', 0)
        return max(delta_t, 0)

    def get_all(self):
      return {k : self.get_name(k) for k in self.timings.keys()}


if __name__ == "__main__":
    main()
