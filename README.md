# parallel-df
Simple local parallel processing for DataFrames. 

For many users, they use Pandas on a single machine instead of running on a compute cluster. Pandas doesn't support parallel processing by default, so those users are stuck with a relatively powerful machine with multi-core CPUs, which they run Pandas on only one of the cores. 

parallel-df is a simple wrapper for any function that takes a Pandas DataFrame object to process. It splits the input DataFrame(s), and uses Python's multiprocessing module to run the tasks on multiple CPU cores. 

# Usage
parallel-df supports two kinds of operations:
1. A function that takes one DataFrame as input, and processes each row/column independently


2. A function that takes a list of DataFrames, and processes each DataFrame independently
