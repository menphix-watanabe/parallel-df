#!/usr/bin/env python3
import sys
import datetime
import pandas as pd
import random
import os
import math
import time
import parallel_df

INPUT_FILE = 'input_data.csv'
INPUT_DATAFRAMES_COUNT = 5

def genData():
  '''Generate a csv file that has 50,000 columns and date range from 1997 to 2018'''
  idx = list()
  cyear = 1997
  cmonth = 1
  while cyear < 2019:
    idx.append(datetime.datetime(year=cyear, month=cmonth, day=1))
    cmonth += 1
    if cmonth > 12:
      cmonth = 1
      cyear += 1

  df = pd.DataFrame(columns=range(50000), index=idx)

  for i in df.columns:
    for index, row in df.iterrows():
      row[i] = random.random()

  return df

def genData2():
  '''Generate several csv files, where each contains 10,000 columns and date range from 1997 to 2018'''
  idx = list()
  cyear = 1997
  cmonth = 1
  while cyear < 2019:
    idx.append(datetime.datetime(year=cyear, month=cmonth, day=1))
    cmonth += 1
    if cmonth > 12:
      cmonth = 1
      cyear += 1

  dfs = [ pd.DataFrame(columns=range(10000), index=idx) for _ in range(INPUT_DATAFRAMES_COUNT) ]

  for df in dfs:
    for i in df.columns:
      for index, row in df.iterrows():
        row[i] = random.random()

  return dfs

def processColumn(coldata):
  s = 0
  for i in coldata:
    i = float(i)
    s = math.sqrt(s ** 2 + i ** 2)
  ps = int(s * 1000)
  factors = list()
  n = 2
  while n < ps / 2:
    if n % ps == 0:
      ps = ps / n
      factors.append(n)
    else:
      n += 1

  return s

def processDataFrame(df):
  rdf = pd.DataFrame(columns=df.columns, index=['results'])
  for c in df.columns:
    rdf[c] = processColumn(df[c])
  return rdf

def main():
  # Parallel DataFrames
  print("Running multiple processes (DataFrame) ... ")
  now = time.time()
  results = parallel_df.runParallelDataFrames(df_idx=0, njobs=None, func=processDataFrame, args=[dfs])
  tdelta = time.time() - now
  print("Multiple processes used {0} seconds".format(tdelta))

  print("Running single process (DataFrame) ... ")
  now = time.time()
  results_s = list()
  for df in dfs:
    results_s.append(processDataFrame(df))
  tdelta = time.time() - now
  print("Single process used {0} seconds".format(tdelta))

  # Verify
  for idx, rdf_s in enumerate(results_s):
    rdf = results[idx]
    for c in rdf_s.columns:
      assert (rdf[c][0] == rdf_s[c][0])
  print("Verified that the results look ok")

  # Parallel Columns
  print("Running multiple processes ... ")
  now = time.time()
  rdf = parallel_df.runParallelColumns(njobs=None, df_idx=0, return_method='data_frame', func=processDataFrame, args=[df])
  tdelta = time.time() - now
  print("Multiple processes used {0} seconds".format(tdelta))

  print("Running single process ... ")
  now = time.time()
  rdf_s = processDataFrame(df)
  tdelta = time.time() - now
  print("Single process used {0} seconds".format(tdelta))

  # Verify
  for c in rdf_s.columns:
    assert(rdf[c][0] == rdf_s[c][0])
  print("Verified that the results look ok")

if __name__ == "__main__":
  global df
  global dfs
  if os.path.exists(INPUT_FILE):
    print("Loading {0} ... ".format(INPUT_FILE))
    df = pd.read_csv(INPUT_FILE, index_col=0, header=0)
    print("done")
  else:
    df = genData()
    print("Writing to {0} ... ".format(INPUT_FILE))
    df.to_csv(INPUT_FILE)

  if os.path.exists('input_df_1.csv'):
    dfs = list()
    print("Loading DataFrames ... ")
    for i in range(INPUT_DATAFRAMES_COUNT):
      fname = "input_df_{0}.csv".format(i)
      df = pd.read_csv(fname, index_col=0, header=0)
      dfs.append(df)
    print("done")
  else:
    print("Generating data ... ")
    dfs = genData2()
    for i, df in enumerate(dfs):
      fname = "input_df_{0}.csv".format(i)
      df.to_csv(fname)
    print("done")

  sys.exit(main())