## parallel-df
Simple local parallel processing for DataFrames. 

For many users, they use Pandas on a single machine instead of running on a compute cluster. Pandas doesn't support parallel processing by default, so those users are stuck with a relatively powerful machine with multi-core CPUs, which they run Pandas on only one of the cores. 

parallel-df is a simple wrapper for any function that takes a Pandas DataFrame object to process. It splits the input DataFrame(s), and uses Python's multiprocessing module to run the tasks on multiple CPU cores. 

## Usage
parallel-df supports two kinds of operations:

**A function that takes one DataFrame as input, and processes each row/column independently**
```
runParallelColumns(df_idx=-1, split_axis=1, njobs=None, func=None, return_method='raw', args=[])

 - df_idx: index of the DataFrame object to split and run parallelly
 - split_axis: along which axis to split the DataFrame, 0 means split by row, 1 means split by column
 - njobs: number of jobs, if None is given then defaults to number of CPU cores
 - func: function to call
 - return method:
     'raw': return a list of processed results from func
     'data_frame': return a DataFrame that's the concatenation of the results from *func*, the results returned from func must be of type DataFrame. The concatenation will be performed along split_axis
 - args: arguments that used to call func
```

Example: 
Assume *myFunc* takes three arguments: df, arg1, and arg2, and returns a DataFrame object. 
```
# Single process: 
myFunc(df, arg1, arg2)
```

```
# Multiple processes:
runParallelColumns(df_idx=0, split_axis=1, func=myFunc, return_method='data_frame', args=[df, arg1, arg2])
# Here df_idx=0 because df is the first argument when calling myFunc
```

**A function that takes a list of DataFrames as input, and processes each DataFrame independently**
```
runParallelDataFrames(df_idx=-1, njobs=None, func=None, args=[])

 - df_idx: index of the DataFrame object to split and run parallelly
 - njobs: number of jobs, if None is given then defaults to number of CPU cores
 - func: function to call
 - args: arguments that used to call func, where the DataFrame object is replaced by a list of DataFrame objects
```

Assume *myFunc* takes three arguments: df, arg1, and arg2, and returns a DataFrame object. 
```
# Single process: 
results = list()
for df in df_list:
    results.append(myFunc(df, arg1, arg2))
```

```
# Multiple processes:
runParallelDataFrames(df_idx=0, func=myFunc, args=[df_list, arg1, arg2])
# Note how df_list replaces df as the first argument in args
```

## Sample Code
*parallel_test.py* contains sample code that runs the two methods described above. 
From testing on my MacBook Pro, which has 4 physical cores and 8 virtual cores, the speedup is significant:

```
Running multiple processes (DataFrame) ... 
Multiple processes used 25.10401701927185 seconds
Running single process (DataFrame) ... 
Single process used 55.779911041259766 seconds
Verified that the results look ok
Running multiple processes ... 
Multiple processes used 5.296130180358887 seconds
Running single process ... 
Single process used 10.57194709777832 seconds
Verified that the results look ok
```

Running with multiple processes is about twice as fast as single thread. 