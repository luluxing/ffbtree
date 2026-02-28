import numpy as np
import sys
import matplotlib.pyplot as plt

def read_file(file):
  latency_data = []
  io_data = []
  with open(file, 'r') as f:
    for line in f:
      # If this line contains "us" and has 4 words seprated by space
      # Extract the first word as an integer and store in a list of latency
      # Extract the third word as an integer and store in a list of io
      if "us" in line and len(line.split()) == 4:
        latency = int(line.split()[0])
        io = int(line.split()[2])
        latency_data.append(latency)
        io_data.append(io)
    return latency_data, io_data

def compute_stats(data):
  # Compute min, max, mean, median, and an approximate mode (histogram peak)
  arr = np.asarray(data)
  if arr.size == 0:
    return None
  data_min = float(np.min(arr))
  data_max = float(np.max(arr))
  data_mean = float(np.mean(arr))
  data_median = float(np.median(arr))
  counts, bin_edges = np.histogram(arr, bins='auto')
  if counts.size > 0:
    peak_idx = int(np.argmax(counts))
    data_mode = float(0.5 * (bin_edges[peak_idx] + bin_edges[peak_idx + 1]))
  else:
    data_mode = data_median
  data_std = float(np.std(arr))
  print(f"min={data_min}, max={data_max}, mean={data_mean}, median={data_median}, mode={data_mode}, std={data_std}")

def plot_pdf(data, label):
  # Plot a normalized histogram (PDF) and annotate basic stats
  print(f"label={label}")
  stats = compute_stats(data)
  # sorted_data = np.sort(np.array(data))
  # # Skip any data that is greater than 100
  # sorted_data = sorted_data[sorted_data <= 100]
  # plt.hist(sorted_data, bins='auto', label=f"{label}", density=True, histtype='step', linewidth=1.5)


def plt_latency_pdf(normal_latency, fsplit_latency, blink_latency, ccbtree_latency):
  # Given a list of latency, plot the cdf of the latency
  plt.figure(figsize=(16, 5))

  plot_pdf(normal_latency, 'normal')
  plot_pdf(fsplit_latency, 'fsplit')
  plot_pdf(blink_latency, 'blink')
  plot_pdf(ccbtree_latency, 'ccbtree')

  plt.xlabel('Latency (us)')
  plt.ylabel('PDF')
  plt.legend()
  plt.grid(True, linestyle='--', alpha=0.4)
  plt.tight_layout()
  plt.title('Latency PDF')
  plt.savefig('latency_pdf.png')

def plt_io_pdf(normal_io, fsplit_io, blink_io, ccbtree_io):
  # Given a list of io counts, plot the cdf
  plt.figure(figsize=(16, 5))

  plot_pdf(normal_io, 'normal')
  plot_pdf(fsplit_io, 'fsplit')
  plot_pdf(blink_io, 'blink')
  plot_pdf(ccbtree_io, 'ccbtree')

  plt.xlabel('IO count')
  plt.ylabel('PDF')
  plt.legend()
  plt.grid(True, linestyle='--', alpha=0.4)
  plt.tight_layout()
  plt.title('IO PDF')
  plt.savefig('io_pdf.png')

def compute_tail(data, tail_percent):
  # Given a list of data, compute the tail percent of the data
  sorted_data = np.sort(np.array(data))
  return sorted_data[int(len(sorted_data) * tail_percent)]

def get_tail(data):
  sorted_data = np.sort(np.array(data))
  return sorted_data[-1]

def plt_all():
  normal_file = sys.argv[1]
  fsplit_file = sys.argv[2]
  blink_file = sys.argv[3]
  ccbtree_file = sys.argv[4]

  normal_latency, normal_io = read_file(normal_file)
  fsplit_latency, fsplit_io = read_file(fsplit_file)
  blink_latency, blink_io = read_file(blink_file)
  ccbtree_latency, ccbtree_io = read_file(ccbtree_file)

  print("latency")
  plt_latency_pdf(normal_latency, fsplit_latency, blink_latency, ccbtree_latency)
  print("io")
  plt_io_pdf(normal_io, fsplit_io, blink_io, ccbtree_io)
  # print(compute_tail(normal_latency, 0.99))
  # print(compute_tail(fsplit_latency, 0.99))
  # print(compute_tail(blink_latency, 0.99))
  # print(compute_tail(ccbtree_latency, 0.99))
  # print(compute_tail(normal_io, 0.99))
  # print(compute_tail(fsplit_io, 0.99))
  # print(compute_tail(blink_io, 0.99))
  # print(compute_tail(ccbtree_io, 0.99))
  # print(get_tail(normal_latency))
  # print(get_tail(fsplit_latency))
  # print(get_tail(blink_latency))
  # print(get_tail(ccbtree_latency))
  # print(get_tail(normal_io))
  # print(get_tail(fsplit_io))
  # print(get_tail(blink_io))
  # print(get_tail(ccbtree_io))


def main():
  if len(sys.argv) == 2:
    file = sys.argv[1]
    latency, io = read_file(file)
    print(f"file={file}")
    print("latency")
    compute_stats(latency)
    print("io")
    compute_stats(io)
  elif len(sys.argv) < 5:
    print('Usage: python plt_0916.py <normal_file> <fsplit_file> <blink_file> <ccbtree_file>')
    sys.exit(1)
  else:
    plt_all()

if __name__ == '__main__':
  main()