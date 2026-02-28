# Given a file that each row contains 3 numbers, representing
# min, median and max. Plot in the same figure the min, median and max
# of each row and connect them with lines. The x-axis is the row number, 
# name it as "number of batches"; the y-axis is named the "Latency (ns)".
# Also plot in the same figure a bar plot of the difference between the
# max and min of each row. Name the bar plot as "Latency range (ns)".

import matplotlib.pyplot as plt
import sys


def plt_all(file):
  with open(file, 'r') as f:
    lines = f.readlines()
    min_data = []
    median_data = []
    max_data = []
    for line in lines:
        data = line.split()
        min_data.append(float(data[0]))
        median_data.append(float(data[1]))
        max_data.append(float(data[2]))
    fig, ax1 = plt.subplots()
    ax1.plot(min_data, label='min')
    ax1.plot(median_data, label='median')
    ax1.plot(max_data, label='max')
    ax1.set_xlabel('number of batches')
    ax1.set_ylabel('Latency (ns)')
    ax1.legend()
    ax2 = ax1.twinx()
    ax2.bar(range(len(min_data)), [max_data[i] - min_data[i] for i in range(len(min_data))], alpha=0.5, label='Latency range (ns)')
    ax2.set_ylabel('Latency range (ns)')
    ax2.legend()
    plt.savefig("tmp_all.png")

def plt_lines(file):
  with open(file, 'r') as f:
    lines = f.readlines()
    min_data = []
    median_data = []
    max_data = []
    for line in lines:
        data = line.split()
        min_data.append(float(data[0]))
        median_data.append(float(data[1]))
        max_data.append(float(data[2]))
    fig, ax1 = plt.subplots()
    ax1.plot(min_data, label='min')
    ax1.plot(median_data, label='median')
    ax1.plot(max_data, label='max')
    ax1.set_xlabel('number of batches')
    ax1.set_ylabel('Latency (ns)')
    ax1.legend()
    plt.savefig("tmp_lines.png")

def plt_bars(file):
  with open(file, 'r') as f:
    lines = f.readlines()
    min_data = []
    median_data = []
    max_data = []
    for line in lines:
        data = line.split()
        min_data.append(float(data[0]))
        median_data.append(float(data[1]))
        max_data.append(float(data[2]))
    fig, ax2 = plt.subplots()
    ax2.bar(range(len(min_data)), [max_data[i] - min_data[i] for i in range(len(min_data))], alpha=0.5, label='Latency range (ns)')
    ax2.set_xlabel('number of batches')
    ax2.set_ylabel('Latency range (ns)')
    ax2.legend()
    plt.savefig("tmp_bars.png")

def plt_time_series(file):
  data = []
  with open(file, 'r') as f:
    for line in f:
      if line.startswith("=====Normal B+-tree"):
        data.append([])
      elif line.startswith("=====FSplitN"):
        data.append([])
      elif line.startswith("=====BFSplit"):
        data.append([])
      elif line.startswith("=====Benchmarking"):
        continue
      else:
        io = int(line.split()[0])
        count = int(line.split()[1])
        temp = [io] * count
        data[-1].extend(temp)
  # Make x-axis in log scale
  plt.xscale('log')
  plt.plot(data[0], label='Normal B+-tree', alpha=0.3)
  plt.plot(data[1], label='FSplitN', alpha=0.3)
  plt.plot(data[2], label='BFSplit', alpha=0.3)
  plt.xlabel('Number of insertions')
  plt.ylabel('Number of IO')
  plt.legend()
  plt.savefig(file +".png")

def count_batch(data, batch_size):
  new_data = []
  for i in range(0, len(data)):
    temp = []
    # for data[i], sum up each batch_size elements
    for j in range(0, len(data[i]), batch_size):
      s = 0
      for k in range(j, j+batch_size):
        s += data[i][k]
      temp.append(s)
    new_data.append(temp)
  return new_data
    

def plt_time_series_batched(file):
  data = []
  batch_size = 1
  with open(file, 'r') as f:
    for line in f:
      if line.startswith("=====Normal B+-tree"):
        data.append([])
      elif line.startswith("=====FSplitN"):
        data.append([])
      elif line.startswith("=====BFSplit"):
        data.append([])
      elif line.startswith("=====Benchmarking"):
        continue
      else:
        io = int(line.split()[0])
        count = int(line.split()[1])
        temp = [io] * count
        data[-1].extend(temp)
  data = count_batch(data, batch_size)
  # Make x-axis in log scale
  plt.xscale('log')
  # plt.plot(data[0], label='Normal B+-tree', alpha=0.5)
  plt.plot(data[1], label='FSplitN', alpha=0.3)
  plt.plot(data[2], label='BFSplit', alpha=0.3)
  plt.xlabel('Number of insertions')
  plt.ylabel('Number of IO')
  plt.legend()
  plt.savefig(file +".png")

def plt_time_series_diff(file, lag):
  """
  Plot differenced time series data mimicking R's diff function.
  
  Parameters:
  - file: Input file path
  - lag: Lag parameter for differencing
         Computes x[i] - x[i-lag] for each element
  """
  data = []
  batch_size = 5
  with open(file, 'r') as f:
    for line in f:
      if line.startswith("=====Normal B+-tree"):
        data.append([])
      elif line.startswith("=====FSplitN"):
        data.append([])
      elif line.startswith("=====BFSplit"):
        data.append([])
      elif line.startswith("=====Benchmarking"):
        continue
      else:
        io = int(line.split()[0])
        count = int(line.split()[1])
        temp = [io] * count
        data[-1].extend(temp)
  
  data = count_batch(data, batch_size)
  
  # Apply differencing with specified lag (mimicking R's diff function)
  # diff(x, lag) computes x[i] - x[i-lag], resulting in length n-lag
  def apply_diff(series, lag):
    if len(series) <= lag:
      return []
    return [series[i] - series[i-lag] for i in range(lag, len(series))]
  
  # Apply differencing to each data series
  diff_data = [apply_diff(series, lag) for series in data]
  
  # Make x-axis in log scale
  # plt.xscale('log')
  # plt.plot(diff_data[0], label=f'Normal B+-tree (diff, lag={lag})', alpha=0.5)
  if len(diff_data) > 1 and len(diff_data[1]) > 0:
    plt.plot(diff_data[1], label=f'FSplitN (diff, lag={lag})', alpha=0.3)
  if len(diff_data) > 2 and len(diff_data[2]) > 0:
    plt.plot(diff_data[2], label=f'BFSplit (diff, lag={lag})', alpha=0.3)
  plt.xlabel('Number of insertions')
  plt.ylabel(f'Differenced IO (lag={lag})')
  plt.legend()
  plt.savefig(file + f"_diff_lag{lag}.png")

def plt_time_series_tree_level(file):
  data = []
  levels = []
  with open(file, 'r') as f:
    for line in f:
      if line.startswith("=====Normal B+-tree"):
        data.append([])
        levels.append([])
      elif line.startswith("=====FSplitN"):
        data.append([])
        levels.append([])
      elif line.startswith("=====BFSplit"):
        data.append([])
        levels.append([])
      elif line.startswith("=====Benchmarking"):
        continue
      else:
        io = int(line.split()[0])
        count = int(line.split()[1])
        level = int(line.split()[2])
        temp = [io] * count
        temp1 = [level] * count
        data[-1].extend(temp)
        levels[-1].extend(temp1)
  # Make x-axis in log scale
  plt.xscale('log')
  # plt.plot(data[0], label='Normal B+-tree', alpha=0.5)
  plt.plot(data[1], label='FSplitN', alpha=0.3, color='red')
  plt.plot(levels[1], label='FSplitNH', alpha=0.3, color='red')
  # plt.plot(data[2], label='BFSplit', alpha=0.3, color='blue')
  # plt.plot(levels[2], label='BFSplitH', alpha=0.3, color='blue')
  plt.xlabel('Number of insertions')
  plt.ylabel('Number of IO')
  plt.legend()
  plt.savefig(file +".png")

def main():
  plt_time_series(sys.argv[1])
  # plt_time_series_batched(sys.argv[1])
  # plt_time_series_diff(sys.argv[1], 10)
  # plt_time_series_tree_level(sys.argv[1])

if __name__ == "__main__":
  main()