#!/home/ubuntu/miniconda2/bin/python
import sys


def read_reduce_output(file, separator=None):
    for line in file:
        yield line.rstrip().split(separator)


def main(separator='\t'):
    data = read_reduce_output(sys.stdin, separator=separator)
    for line in data:
        print "%s\t%f" % (line[0], float(line[1]))


if __name__ == "__main__":
    main()
