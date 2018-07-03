from spark_benchmark_hpc import *
import numpy as np

timers = SimpleTimer()

args = parse_args()

timers.init_and_start("generate")
data = generate(0, args.block_size * MEGA_MULTIPLIER)   # 0 is the RDD ID (can be anyting)
timers.stop("generate")


timers.init_and_start("shift")
shift = np.array([25.25, -12.125, 6.333], dtype=np.float64)
shifted = do_shift(data, shift)
timers.stop("shift")


timers.init_and_start("average")
average = do_average(shifted)
timers.stop("average")

print(timers.get_all())




