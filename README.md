# duduckdb

duduckdb prints a summary of disk or inodes usage, using an
interface that is similar, but not identical, to the
[du](https://man7.org/linux/man-pages/man1/du.1.html) (disk usage) 
Unix tool. A crucial difference is that instead of directly querying a file
system, information is read from a parquet file containing an index of the
directory in question.

For each of the existing staging directories, we generate such a parquet
database. You can find all databases in `/data/leuven/public/staging-stats`.
Note that the owner and group from your staging storage is automatically assigned
to the database as well. Together with limited permissions, this only allows
queries from users that are part of the matching staging group.

## Getting started

duduckdb is available as a module on the cluster (currently only on the login node):

```
$ module load duduckdb
```

Note that this repository contains the duduckdb package itself, which allows you to
install it in your own directories as well. Unless you are trying out a newer version
not available on the cluster, or developing on this code, there is no real need for
this local installation however.

## Usage

Use the `--help` flag to get an overview of the possible command line options:

```
$ duduckdb --help
usage: duduckdb [-h] [-d MAX_DEPTH] [--min-depth MIN_DEPTH] [--disk-usage | --no-disk-usage] [--inodes | --no-inodes] [--human-readable] [--si-units] [--top-directory TOP_DIRECTORY]
                [--per-user] [--sort-by SORT_BY] [--older-than OLDER_THAN] [--newer-than NEWER_THAN] [--timestamp-type {atime,mtime,ctime}] [--nthreads NTHREADS] [--debug]
                fn

Summarize disk usage information contained in parquet file.
...
```

Without any optional arguments, you get disk usage in bytes and inode count
for the root directory and each of its subdirectories:

```
$ duduckdb /data/leuven/public/staging-stats/<your-staging-dir>.parquet
directory:               size             inodes
================================================================================
:                        3268758550858    132323
subdir2:                2964581439838    13079
subdir1:                74795247         165
subdir3:                1864536          34
...
```

The sizes will be nicer to read by supplying `--human-readable`:

```
$ duduckdb /data/leuven/public/staging-stats/<your-staging-dir>.parquet --human-readable
directory:               size             inodes
================================================================================
:                        3.0TiB           129.2Ki
subdir2:                2.7TiB           12.8Ki
subdir1:                71.3MiB          165.0
subdir3:                1.8MiB           34.0
...
```

Deeper directories can be listed by increasing `--max-depth`:

```
$ duduckdb /data/leuven/public/staging-stats/<your-staging-dir>.parquet --human-readable --max-depth=2
directory:               size             inodes
================================================================================
:                       3.0TiB           129.2Ki
subdir1:                71.3MiB          165.0
...
subdir2/subdir1_2:      465.7GiB         2.5Ki
subdir2/subdir1_3:      46.6GiB          2.5Ki
subdir2/subdir1_1:      7.5GiB           405.0
...
```

The sorting can be controlled with the `--sort-by` option. It defaults to
`depth,size`, which means output lines are first sorted by depth and within
each depth by size (descending). If you want to sort for instance by number
of inodes for each depth:

```
$ duduckdb /data/leuven/public/staging-stats/<your-staging-dir>.parquet --human-readable --sort-by=depth,inodes
directory:               size             inodes
================================================================================
:                        3.0TiB           129.2Ki
subdir2:                2.7TiB           12.8Ki
subdir1:                71.3MiB          165.0
subdir3:                1.8MiB           34.0
...
```

With `--per-user`, usage per user is reported:

```
$ duduckdb /data/leuven/public/staging-stats/<your-staging-dir>.parquet --per-user --human-readable
directory:               size             inodes
================================================================================
:                        3.0TiB           129.2Ki
--------------------------------------------------------------------------------
    vsc_id1:             2.8TiB           17.3Ki
    vsc_id5:             159.0GiB         14.0
    vsc_id2:             45.8GiB          111.9Ki
    vsc_id3:             12.0KiB          4.0
    vsc_id4:             16.0KiB          13.0
    vsc_id6:             4.0KiB           1.0
...
```

You can filter files/directories based on their timestamp with `--older-than`
and `--newer-than`. Note that a line is printed for a directory in case *any*
if the files/directories inside it passes this filter:

```
$ duduckdb /data/leuven/public/staging-stats/<your-staging-dir>.parquet --human --older-than=2023-01-01
directory:               size             inodes
================================================================================
:                        159.0GiB         42.0
subdir1:                 -                -
subdir2:                 -                -
subdir3:                 1.8MiB           30.0
```


## Speeding up queries

As this tool relies on making a copy of the parquet database in memory. If you need
to run multiple queries, you can avoid recreating the database each time by making
use of the duduckdb Python interface:

```
$ python3
from duduckdb.duduckdb import DUDB
db = DUDB("/data/leuven/public/staging-stats/<your-staging-dir>.parquet")
db.report_du(max_depth=2, metrics=['size', 'inodes'])
from datetime import datetime
db.report_du(older_than=datetime(2022, 1, 1), timestamp_type='atime', max_depth=1, human_readable=True)
```

The `report_du` function accepts keyword arguments corresponding to optional
arguments of the `duduckdb` program.
