#!/home/ubuntu/miniconda2/bin/python
import sys
from itertools import groupby
from operator import itemgetter


def read_mapper_out(file, separator=None):
    for line in file:
        yield line.rstrip().split(separator)


def main(separator='\t'):
    data = read_mapper_out(sys.stdin, separator=separator)
    for key, group in groupby(data, itemgetter(0)):
        try:
            total = sum((float(val) for key, val in group))
            coordinate = key.split(',')
            print "%d\t%d\t%f" % (int(coordinate[0]), int(coordinate[1]), total)
        except:
            pass


if __name__ == "__main__":
    main()
