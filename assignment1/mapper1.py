#!/home/ubuntu/miniconda2/bin/python
import sys
import os


def read_input(file):
    for line in file:
        yield line.rstrip().split()


def main(separator='\t'):
    try:
        file_path = os.environ['mapreduce_map_input_file']
    except KeyError:
        file_path = os.environ['map_input_file']
    file_name = os.path.split(file_path)[-1]

    data = read_input(sys.stdin)
    if file_name in ["M_small.dat","M_median.dat","M_large.dat"]:
        for line in data:
            # (j M,i,v)
            print "%d\t%s,%d,%f" % (int(line[1]), 'M', int(line[0]), float(line[2]))
    elif file_name in ["N_small.dat","N_median.dat","N_large.dat"]:
        for line in data:
            # (j N,k,w)
            print "%d\t%s,%d,%f" % (int(line[0]), 'N', int(line[1]), float(line[2]))


if __name__ == "__main__":
    main()
