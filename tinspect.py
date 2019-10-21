#!/usr/bin/python
import argparse
import math
import numpy as np
import os
from pathlib import Path

parser = argparse.ArgumentParser(description='''Table inspection tool.
    Shows information about python loading of .csv and .parquet files.''')
parser.add_argument('file', metavar='filename', type=str,
                    help='file or directory path')
parser.add_argument('-c', dest='cast', type=str, default=False,
                    help='copy dataset, optionally casting to different type')
parser.add_argument('-p', dest='partitions', type=int, default=0,
                    help='make N partitions, requires cast, priority over -pr')
parser.add_argument('-pr', dest='partitionrows', type=int, default=0,
                    help='partition with max row length, requires cast')
parser.add_argument('-t', dest='h', action='store_true',
                    help='print top 5 rows (head)')
parser.add_argument('-u', dest='u', action='store_true',
                    help='count unique values and nulls per column')


args = parser.parse_args()


def split_dataframe(dataframe, size=1750000):
    return [dataframe[i*size:(i+1)*size]
            for i in range(math.ceil(len(dataframe)/size))]


def split_dataframe_N(dataframe, count=2):
    total_rows = float(len(dataframe))
    return [dataframe[int(i*total_rows/count):int((i+1)*total_rows/count)]
            for i in range(count)]


def get_size(start_path):
    if os.path.isdir(start_path):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(start_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
        return total_size
    return os.path.getsize(start_path)


def count_lines(filename):
    with open(filename) as f:
        return sum(1 for line in f) - 1  # header


def tinspect(filename):
    filename = Path(filename)
    df = 0

    if filename.suffix == '.csv':

        import pandas as pd
        import csv

        with open(filename, 'r') as csvfile:
            dialect = csv.Sniffer().sniff(csvfile.read(1024*16))
            delim = dialect.delimiter

        if args.cast or args.u:
            df = pd.read_csv(filename, sep=delim)
        else:
            df = pd.read_csv(filename, sep=delim, nrows=10)

        types = df.dtypes
        max_length = max([len(x) for x in types.index])+3
        if args.u:
            nuniques = df.nunique()
            nnull = len(df) - df.count()
            pnull = (nnull / len(df) * 100).apply(np.ceil) / 100
        print(f'''{
                str(chr(10)).join([f"""{x.ljust(max_length)} {
                y}""".ljust(max_length+15)+((str(nuniques[x]).ljust(6)+str(nnull[x]).ljust(8)+str(pnull[x])[:4]) if args.u and x in nuniques else '')
                for x, y in types.iteritems()])
                    }{
                chr(10)
                    }{
                '{:,}'.format(count_lines(filename))
                    } rows{
                chr(10)
                    }{
                len(types)
                    } cols''')
        if args.h:
            print(df.head())

    elif filename.suffix == '.parquet':
        from pyarrow.parquet import ParquetFile, ParquetDataset
        from pyarrow.lib import ArrowIOError

        try:
            pf = ParquetFile(filename)
            rows = pf.scan_contents()
        except ArrowIOError as e:
            pf = ParquetDataset(filename)
            df = pf.read()
            rows = df.shape[0]

        types = pf.schema
        max_length = max([len(k.name) for k in pf.schema])+3

        if args.cast or args.h or args.u:
            if not df:
                df = pf.read()
            df = df.to_pandas()

        if args.u:
            nuniques = df.nunique()
            nnull = len(df) - df.count()
            pnull = (nnull / len(df) * 100).apply(np.ceil) / 100
        print(f'''{
                str(chr(10)).join([f"""{k.name.ljust(max_length)} {
                k.physical_type}  {
                ("" if k.logical_type == "NONE" else k.logical_type)
                }""".ljust(max_length+30)+((str(nuniques[k.name]).ljust(6)+str(nnull[k.name]).ljust(8)+str(pnull[k.name])[:4]) if args.u and k.name in nuniques else '')
                for k in types])
                    }{
                chr(10)
                    }{
                '{:,}'.format(rows)
                    } rows{
                chr(10)
                    }{
                len(types)
                    } cols''')
        if args.h:
            print(df.head())
    else:
        print("Unsupported: " + str(filename))
        return
    if args.cast:
        cast_to = Path(args.cast)

        df_list = [df]
        if args.partitions:
            df_list = split_dataframe_N(df, args.partitions)
        elif args.partitionrows:
            df_list = split_dataframe(df, args.partitionrows)

        for i in range(len(df_list)):
            if (len(df_list)>1):
                print(f"Building and exporting table {i}")
                outfile = Path(cast_to.parent /
                               f"{cast_to.stem}_{i}{cast_to.suffix}")
            else:
                print("Exporting table")
                outfile = cast_to
            if cast_to.suffix == ".csv":
                df_list[i].to_csv(outfile, index=False)
            elif cast_to.suffix == ".parquet":
                df_list[i].to_parquet(outfile, index=False)



if os.path.exists(args.file):
    tinspect(args.file)
    print(f'''{'{:,}'.format(get_size(args.file))} bytes''')
else:
    print("No file supplied")
