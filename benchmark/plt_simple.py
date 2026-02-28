import sys
import matplotlib.pyplot as plt

f1 = sys.argv[1]
f2 = sys.argv[2]
f3 = sys.argv[3]

# Read the file except the first and last
with open(f1, 'r') as f:
  lines1 = f.readlines()
  data1 = [int(line.strip()) for line in lines1[1:-1] if line.strip()]

with open(f2, 'r') as f:
  lines2 = f.readlines()
  data2 = [int(line.strip()) for line in lines2[1:-1] if line.strip()]

plt.plot(data1, label='BtreeOLC', alpha=0.5)
plt.plot(data2, label='CCBtreeOLC', alpha=0.5)
plt.legend(loc='upper left')
plt.savefig(f3)