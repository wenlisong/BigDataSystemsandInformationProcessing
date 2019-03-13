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
            M_list = []
            N_list = []
            for key, val in group:
                val = val.split(',')
                if val[0] == 'M':
                    M_list.append([int(val[1]), float(val[2])])
                else:
                    N_list.append([int(val[1]), float(val[2])])
            for i, v in M_list:
                for k, w in N_list:
                    print "%d,%d\t%f" % (i, k, v * w)
        except:
            pass


if __name__ == "__main__":
    main()
