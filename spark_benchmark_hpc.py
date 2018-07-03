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
    print("Array of %s float vectors, time = %s" % (block_count, deltat))
    return array


# Only needed for file I/O, which we're not doing at the moment.
# Bring back later.

def parseVectors(binary_data):
    array = np.fromstring(binary_data, dtype=np.float64)
    array = arr.reshape(arr.shape[0]/3, 3)
    return array


def do_shift(array, vector_displacement):
    for i in range(len(array)):
        array[i] += vector_displacement
    return array


def do_average(array):
    avg = np.array([0.0, 0.0, 0.0])
    for i in range(len(array)):
        avg += array[i]
    avg /= len(array)
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
    parser.add_argument("-s", "--block_size", type=int,
                        default=0, help="block size (number of 3D float vectors x %d)" % MEGA_MULTIPLIER)

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

    # Laziness
    parser.add_argument("-l", "--cache", action="store_true",
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
    timers.init_and_start("rdd A")
    if args.generate:
        gen_num_blocks = args.blocks
        gen_block_size = args.block_size
        x = range(0, gen_num_blocks)
        rdd = sc.parallelize(range(0, gen_num_blocks),
                             args.nodes * args.cores * args.nparts)
        gen_block_count = gen_block_size*MEGA_MULTIPLIER
        A = rdd.map(lambda n: generate(n, gen_block_count))
    else:
        print("either --generate must be specified")
        sc.stop()
        sys.exit(-1)

    timers.stop("rdd A")

    timers.init_and_start("rdd A eval")
    if not args.lazy:
       count = A.count()
       A.cache()
    timers.stop("rdd A eval")

    # apply simple operation (V'=V+V0)

    timers.init_and_start("rdd B")
    shift = np.array([25.25, -12.125, 6.333], dtype=np.float64)
    B = A.map(lambda x: do_shift(x, shift))
    timers.stop("rdd B")

    timers.init_and_start("rdd B eval")
    if not args.lazy:
       count2 = B.count()
       B.cache()
    timers.stop("rdd B eval")

    timers.init_and_start("average")
    C = B.map(do_average)
    if not args.lazy:
       count3 = C.count()
       C.cache()
    timers.stop("average")

    timers.init_and_start("reduce")
    result = C.reduce(lambda x, y: x + y) / count3
    timers.stop("reduce")
    timers.stop("overall")

    variables = {
        'count': count,
        'count2': count2,
        'count3': count3,
        # NumPy array -> Python list (for JSON serialization)
        'result': list(result)

    }

    if args.json:
        results = {}
        results['vars'] = variables
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
