from multiprocessing import Queue, Pool, cpu_count, Manager
import numpy as np
import pandas as pd

def runDataFrame(args):
  job_idx, func, q = args[:3]
  r = func(*args[3:])
  q.put({ 'pid': job_idx, 'results': r })

def runParallelDataFrames(df_idx=-1, njobs=None, func=None, args=[]):
  dfs = args[df_idx]
  # Determine the number of splits.
  # If njobs is not provided, use the number of CPU cores
  if njobs is None:
    njobs = cpu_count()

  m = Manager()
  q = m.Queue()
  pool = Pool(processes=njobs)
  passing_args = list()
  for i in range(len(dfs)):
    # one_arg: job_idx, func, q, arg0, arg1, ... df[i], argx, argy ...
    one_arg = [i, func, q]
    one_arg.extend(args[:df_idx])
    one_arg.append(dfs[i])
    one_arg.extend(args[df_idx+1:])
    passing_args.append(one_arg)
  pool.map(runDataFrame, passing_args)

  # Reduce
  results = list()
  while not q.empty():
    results.append(q.get())
  results.sort(key=lambda x: x['pid'])
  return [ r['results'] for r in results ]

def runParallelColumns(df_idx=-1, split_axis=1, njobs=None, func=None, return_method='raw', args=[]):
  '''
  :param df_idx: which index in args represents the DataFrame object to split
  :param split_axis: along which axis to split
  :param njobs: number of jobs, if None, will use the number of CPU cores
  :param func: function to run
  :param return_method:
    raw: return a list of results
    data_frame: calling pandas.concat() on the resulting DataFrames, axis=split_axis
  :param args: args passed to func, including the DataFrame pointed by df_idx

  :return: results
  '''
  df = args[df_idx]

  # Determine the number of splits.
  # If njobs is not provided, use the number of CPU cores
  if njobs is None:
    njobs = cpu_count()
  dfs = np.array_split(df, njobs, axis=split_axis)

  # Map
  m = Manager()
  q = m.Queue()
  pool = Pool(processes=njobs)
  passing_args = list()
  for i in range(njobs):
    # one_arg: job_idx, func, q, arg0, arg1, ... df[i], argx, argy ...
    one_arg = [i, func, q]
    one_arg.extend(args[:df_idx])
    one_arg.append(dfs[i])
    one_arg.extend(args[df_idx+1:])
    passing_args.append(one_arg)
  pool.map(runDataFrame, passing_args)

  # Reduce
  results = list()
  while not q.empty():
    results.append(q.get())
  results.sort(key=lambda x: x['pid'])

  if return_method == 'raw':
    return [ r['results'] for r in results ]
  elif return_method == 'data_frame':
    rdf = results[0]['results']
    for r in results[1:]:
      rdf = pd.concat([rdf, r['results']], sort=False, axis=split_axis)
    return rdf
  else:
    raise Exception("Unsupported return method: {0}".format(return_method))
