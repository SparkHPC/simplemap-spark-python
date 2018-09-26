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
import math

from pyspark import SparkContext, SparkConf

# Used by RDD lambda to generate the work

MEGA_MULTIPLIER = 1024 * 1024

def generate(n, block_count):
    seed = int(time.time()/(n+1))
    np.random.seed(seed)
    a, b = -1000, 1000
    timers = SimpleTimer()
    timers.init_and_start("generate")
    array = (b-a)*np.random.random_sample((block_count, 3))+a
    timers.stop("generate")
    deltat = timers.get_name("generate")
    print("Array (block id=%s) of %s float vectors, time = %s" % (n, block_count, deltat))
    return array


# Only needed for file I/O, which we're not doing at the moment.
# Bring back later.

def parseVectors(binary_data):
    array = np.fromstring(binary_data, dtype=np.float64)
    array = arr.reshape(arr.shape[0]/3, 3)
    return array


def do_shift(array, vector_displacement):
    array += vector_displacement
    return array


def do_average(array):
    avg = np.mean(array, axis=0)
    return avg


def noop(x):
    pass  # print("noop")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Simple Map Spark Microbenchmark - Python Version")

    # Options for generating RDD (in memory)
    parser.add_argument("-g", "--generate", action="store_true",
                        default=False, help="generate data (boolean)")
    parser.add_argument("-b", "--blocks", type=int,
                        default=0, help="number of blocks")
    parser.add_argument('-m', "--multiplier", type=int, help="block size multiplier as an integer (default %d = 2^%d)" % (MEGA_MULTIPLIER, math.log2(MEGA_MULTIPLIER)), default=MEGA_MULTIPLIER)

    parser.add_argument("-k", "--block_size", type=int,
                        default=0, help="block size (number of 3D float vectors times value of --multiplier)")

    # These are all used to define amount of paralleism (for sc.parallelize).

    parser.add_argument("-n", "--nodes", type=int,
                        default=1, help="number of nodes (for reporting)")
    parser.add_argument("-p", "--nparts", type=int, default=1,
                        help="how many partitions to create per node")
    parser.add_argument("-c", "--cores", type=int,
                        default=12, help="number of cores per CPU")

    # Reporting
    parser.add_argument('-j', "--json", type=str,
                        default=None, help="where to write the report")

    # Laziness
    parser.add_argument("-l", "--lazy", action="store_true",
                        default=False, help="cache data (for timing intermediate RDD map computations")

    # parser.add_argument("-z", "--size", type=int, required=True,
    #                    help="input size (in mb, for reporting)")
    return parser.parse_args()


def main():

    sconf = SparkConf()
    sc = SparkContext(appName="simplemap-spark-python", conf=sconf)
    python_version = sys.version
    spark_python_version = sc.pythonVer
    args = parse_args()
    timers = SimpleTimer()

    # read input files or generate input data
    timers.init_and_start("overall")
    timers.init_and_start("map")
    if args.generate:
        gen_num_blocks = args.blocks
        gen_block_size = args.block_size
        x = range(0, gen_num_blocks)
        rdd = sc.parallelize(range(0, gen_num_blocks),
                             args.nodes * args.cores * args.nparts)
        gen_block_count = gen_block_size*args.multiplier
        A = rdd.map(lambda n: generate(n, gen_block_count))
    else:
        print("either --generate must be specified")
        sc.stop()
        sys.exit(-1)

    if not args.lazy:
       A.cache()
       count = A.count()
    timers.stop("map")

    # apply simple operation (V'=V+V0)

    timers.init_and_start("shift")
    shift = np.array([25.25, -12.125, 6.333], dtype=np.float64)
    B = A.map(lambda x: do_shift(x, shift))

    if not args.lazy:
       B.cache()
       count2 = B.count()
    timers.stop("shift")

    timers.init_and_start("average")
    C = B.map(do_average)
    if not args.lazy:
       C.cache()
       count3 = C.count()
    timers.stop("average")

    timers.init_and_start("reduce")
    result = C.reduce(lambda x, y: x + y) / gen_num_blocks
    timers.stop("reduce")
    timers.stop("overall")

    if args.json:
        results = {}
        results['experiment'] = { 'id' : 'simplemap-spark-python' }
        results['args'] = vars(args)
        results['performance'] = timers.get_all()
        with open(args.json, "w") as report:
            text = json.dumps(results, indent=4, sort_keys=True)
            report.write(text + "\n")
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
        return {k: self.get_name(k) for k in self.timings.keys()}


if __name__ == "__main__":
    main()
