# Getting Started

This code is fairly new. Please contact me (gkt@cs.luc.edu) if you have any questions.

You will need Apache Spark to run this code.

```shell
usage: spark_benchmark_hpc.py [-h] [-g] [-b BLOCKS] [-s BLOCK_SIZE] -n NODES
                              [-p NPARTS] [-c CORES] [-j JSON]

Simple Map Spark Microbenchmark - Python Version

optional arguments:
  -h, --help            show this help message and exit
  -g, --generate        generate data (boolean)
  -b BLOCKS, --blocks BLOCKS
                        number of blocks
  -s BLOCK_SIZE, --block_size BLOCK_SIZE
                        block size
  -n NODES, --nodes NODES
                        number of nodes (for reporting)
  -p NPARTS, --nparts NPARTS
                        how many partitions to create per node
  -c CORES, --cores CORES
                        number of cores per CPU
  -j JSON, --json JSON  where to write the report
```


