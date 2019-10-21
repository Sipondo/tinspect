# tinspect
Parquet/csv table CLI inspection and conversion tool.

## Requirements

```
argparse
math
numpy
os
pathlib

Additionally, pyarrow is required for opening and saving .parquet files.
```

## Arguments
```
Table inspection tool. Shows information about python loading of .csv and
.parquet files.

positional arguments:
  filename           file or directory path

optional arguments:
  -h, --help         show this help message and exit
  -c CAST            copy dataset, optionally casting to different type
  -p PARTITIONS      make N partitions, requires cast, priority over -pr
  -pr PARTITIONROWS  partition with max row length, requires cast
  -t                 print top 5 rows (head)
  -u                 count unique values and nulls per column
```
