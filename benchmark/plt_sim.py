# Given a file that contains lines like "Inner split: x; leaf splits: y"
# Extract the values of x and y and plot their summation in a line plot.
# Name the x-axis as "Number of insertions" and the y-axis as "Number of splits".
# There are lines that start with "====". When encountered, start a new array
# of data to plot.
#

import matplotlib.pyplot as plt
import sys
from brokenaxes import brokenaxes

width = 8
height = 4

colors = ['b', 'r', 'c', 'm', 'y', 'k']

def plt_pts(data, output_file):
    # Make the figure to be larger in width
    fig, ax1 = plt.subplots(figsize=(width, height))
    for i in range(len(data)):
        d = data[i]
        ax1.scatter(range(len(d)), d, color=colors[i], label='Data ' + str(i), alpha=0.5)
    ax1.legend(loc='upper left')
    ax1.set_xlabel('Number of insertions')
    ax1.set_ylabel('Number of splits')
    plt.savefig(output_file)

def plt_bars(data, output_file):
    # Create a histogram of the data
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(width, height), gridspec_kw={'height_ratios': [4, 3]})

    labels = ['Normal', 'F-Split-1', 'F-Split-N', 'S-Split']
    
    # Plot the lower part of the data
    ax1.hist(data, bins=10, histtype='bar', color=colors[:len(data)], label=labels)
    ax1.set_ylim(10, len(data[0]) + 10)
    ax1.legend(loc='upper right')
    ax1.set_ylabel('Number of nodes')
    
    # Plot the upper part of the data
    ax2.hist(data, bins=10, histtype='bar', color=colors[:len(data)], label=labels)
    ax2.set_ylim(0, 10)
    ax2.set_xlabel('Number of splits')
    ax2.set_ylabel('Number of nodes')
    
    # Adjust the position of the subplots to create the broken axis effect
    ax1.spines['bottom'].set_visible(False)
    ax2.spines['top'].set_visible(False)
    ax1.xaxis.tick_top()
    ax1.tick_params(labeltop=False)
    ax2.xaxis.tick_bottom()
    
    d = .005  # how big to make the diagonal lines in axes coordinates
    kwargs = dict(transform=ax1.transAxes, color='k', clip_on=False)
    ax1.plot((-d, +d), (-d, +d), **kwargs)        # top-left diagonal
    ax1.plot((1 - d, 1 + d), (-d, +d), **kwargs)  # top-right diagonal
    
    kwargs.update(transform=ax2.transAxes)  # switch to the bottom axes
    ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)  # bottom-left diagonal
    ax2.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)  # bottom-right diagonal
    
    plt.savefig(output_file)

file = sys.argv[1]
with open(file, 'r') as f:
  lines = f.readlines()
  data = []
  record = True
  first_line = True
  for line in lines:
      if line.startswith("===="):
          if line.startswith("=====Ben"):
              record = False
          else:
              record = True
              first_line = True
              data.append([])
      else:
          if record:
            # Need to skip the first line that follows the "====" line
            if first_line:
                first_line = False
            else:
              data[-1].append(sum([int(x) for x in line.split() if x.isdigit()]))

# plt_pts(data, file + "_pts.png")
plt_bars(data, file + "_bars.png")