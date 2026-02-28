# Given a file with two columns, read the first column in a list
# Plot the first column with x-axis as the row number.

import matplotlib.pyplot as plt
import sys
import numpy as np

def read_file(file, colnum):
    f = open(file, 'r')
    secs = []
    for line in f:
        if line.startswith('All'):
            continue
        words = line.split()
        secs.append(float(words[colnum]))
    f.close()
    return secs

def plt_all(files, colnum, ylabel, figname, start=0):
    data = []
    for file in files:
        data.append(read_file(file, colnum))

    fig, ax1 = plt.subplots(figsize=(30, 6))
    colors = ['r', 'g', 'b', 'c', 'm', 'y']
    legends = ['Btreenormal', 'fsplit', 'Blink', 'CCbtree']
    new_legends = []
    for i, secs in enumerate(data):
        # ax1.scatter(range(len(secs)), secs, label=f'File {i+1}', color=colors[i % len(colors)], alpha=0.7)
        secs = secs[start:]
        # Add a horizontal line at the maximum value
        max_value = max(secs)
        new_legends.append(legends[i % len(colors)] + f' {max_value:.2f}')
        ax1.axhline(y=max_value, color=colors[i % len(colors)], linestyle='--', alpha=0.5)
        ax1.plot(secs, label=f'File {i+1}', color=colors[i % len(colors)], alpha=0.7)
    ax1.set_xlabel('Insertion Number')
    ax1.set_ylabel(ylabel)
    ax1.legend(new_legends[:len(data)])
    plt.savefig(figname)

def plt_distribution(files, colnum, xlabel, ylabel, figname, num_bins, start=0):
    data = []
    for file in files:
        data.append(read_file(file, colnum))

    fig, ax1 = plt.subplots(figsize=(12, 6))
    colors = ['r', 'g', 'b', 'c', 'm', 'y']
    legends = ['Btreenormal', 'fsplit', 'Blink', 'CCbtree']

    # plot the data distribution in a histogram without overlapping bars
    x_multi = []
    for i, secs in enumerate(data):
      secs = secs[start:]
      x_multi.append(secs)
    ax1.hist(x_multi, num_bins, histtype='bar')
    ax1.set_xlabel(xlabel)
    ax1.set_ylabel(ylabel)
    ax1.set_yscale('log')
    ax1.legend(legends[:len(data)])
    plt.savefig(figname)

def main():
    file_num = int(sys.argv[1])
    files = []
    for i in range(file_num):
        files.append(sys.argv[i + 2])
    plt_all(files, 0, 'Latency (ns)', 'seq_time.png', 10000)
    plt_all(files, 2, 'Number of IO', 'seq_io.png', 10000)
    plt_distribution(files, 0, 'Latency (ns)', 'Count', 'seq_time_distribution.png', 30, 10000)
    plt_distribution(files, 2, 'Number of IO', 'Count', 'seq_io_distribution.png', 30, 10000)
    
if __name__ == "__main__":
    main()