# Given a file with lines like "Insert-time=x, lookup-time=y, insert-io=z, height=w"
# Plot the insert-time and lookup-time in the line plot.
# The x-axis is the number of insertions.
# The y-axis is time in milliseconds.

import matplotlib.pyplot as plt
import sys

def read_file(file):
  data = [[],[]]
  max_io = 0
  with open(file, 'r') as f:
    for line in f:
      if line.startswith("Insert-time="):
        insert_time = int(line.split("=")[1].split(",")[0])
        lookup_time = int(line.split("=")[2].split(",")[0])
        insert_io = int(line.split("=")[3].split(",")[0])
        height = int(line.split("=")[4].split(",")[0])
        max_io = max(max_io, insert_io)
        data[0].append(insert_time)
        data[1].append(lookup_time)
  print("max_io: ", max_io)
  return data

def plt_insert_query_time(file):
  data = read_file(file)
  plt.plot(data[0], label="Insert-time")
  plt.plot(data[1], label="Lookup-time")
  plt.xlabel("Number of insertions")
  plt.ylabel("Time in nanoseconds")
  plt.legend()
  plt.savefig(file + ".png")

file = sys.argv[1]
plt_insert_query_time(file)