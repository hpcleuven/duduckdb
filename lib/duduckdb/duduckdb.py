#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
from datetime import datetime
import time
import pwd
import duckdb


def get_headers(fn):
    '''Extract the names and types of the columns in a database'''
    columns = duckdb.sql(f"describe select * from '{fn}';").fetchall()
    labels = [c[0] for c in columns]
    types = [c[1] for c in columns]
    return labels, types


def get_metadata(fn):
    '''Extract key-value metadata as dictionary'''
    kvs = duckdb.sql(f"select * from parquet_kv_metadata('{fn}');").fetchall()
    metadata = {}
    for _, key, value in kvs:
        metadata[key.decode("utf-8")] = value.decode("utf-8")
    return metadata


def sizeof_fmt(num, si_units=False, suffix="B", formatter='.1f'):
    '''Return a human-readable string representing a number of bytes'''
    if si_units:
        fac = 10.0**3
        units = ["", "k", "M", "G", "T", "P", "E"]
    else:
        fac = 2.0**10
        units = ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei"]
    for unit in units:
        if abs(num) < fac:
            return f"{num:{formatter}}{unit}{suffix}"
        num /= fac
    return f"{num:{formatter}}{units[-1]}{suffix}"


def print_usage_single(sizes, identifier, human_readable=False, si_units=False,
                       suffixes=["B"], prefix="", formatter='.1f'):
    if human_readable:
        assert len(sizes) == len(suffixes)
    msg = f'{prefix+identifier+":":24}'
    for size, suffix in zip(sizes, suffixes):
        if size is None or size == 0:
            sizestr = '-'
        else:
            if human_readable:
                sizestr = sizeof_fmt(size, si_units=si_units, suffix=suffix,
                                     formatter=formatter)
            else:
                sizestr = f'{size}'
        msg += f' {sizestr:16}'
    print(msg)


class DUDB(object):
    def __init__(self, fn, debug=False, nthreads=4):
        self.fn = fn
        self.debug = debug
        self.nthreads = nthreads
        self.read_db(self.fn)

    def read_db(self, fn):
        # Report age of parquet file
        metadata = get_metadata(fn)
        if 'timestamp' in metadata.keys():
            timestamp = metadata['timestamp'].split('m=')[0]
            logging.warning(f'The data was collected on {timestamp}')
        else:
            mtime = time.ctime(os.path.getmtime(fn))
            mtime_str = datetime.strptime(mtime, "%a %b %d %H:%M:%S %Y")
            logging.warning(f'The file {fn} was last modified on {mtime_str}')
        logging.warning('The reported usage is a snapshot of the usage around '
                        'that time')

        # Set up an in-memory connection and initialise it with parquet file
        # content
        self.conn = duckdb.connect(database=":memory:",
                                   config={'threads': self.nthreads})
        if self.debug:
            self.conn.execute("set enable_progress_bar = true;")

        logging.debug('Creating database in memory...')
        tstart = time.time()
        columns = '*'
        self.conn.execute(f"create table index as select {columns} from "
                          f"'{fn}';")
        logging.debug(f'... done, took {time.time() - tstart:.3f}s')

    def report_du(self, top_directory="", per_user=False, older_than=None,
                  newer_than=None, max_depth=1, min_depth=0, metrics=['size'],
                  human_readable=False, si_units=False,
                  timestamp_type='atime', suppress_output=False,
                  sort_by=['depth', 'size']):
        # Check that required columns are present
        required_columns = ['path', 'is_dir', 'size']
        if per_user:
            required_columns.append('uid')
        if older_than or newer_than:
            assert timestamp_type, 'timestamp_type is required if ' \
                                   'older/newer_than is provided'
            required_columns.append(timestamp_type)
        labels, types = get_headers(self.fn)
        logging.debug(f"Found columns with labels {', '.join(labels)}")
        for label in required_columns:
            assert label in labels, f'Required column {label} is missing.'

        # Add a column representing filesystem depth by counting "/"
        top_directory_stripped = top_directory.rstrip(os.sep)
        top_directory_depth = top_directory_stripped.count(os.sep)
        if top_directory_stripped == "":
            top_directory_depth -= 1
        self.conn.execute("alter table index add if not exists depth "
                          "integer;")
        self.conn.execute("update index set depth = "
                          "len(path) - len(replace(path, '/', '')) "
                          f"- {top_directory_depth};")
        # Special case of root directory
        if top_directory_stripped == "":
            query = "update index set depth = 0 where path = '.' or path = '';"
            self.conn.execute(query)

        # Print column headers
        if not suppress_output:
            print_usage_single(metrics, "directory",
                               suffixes=[""] * len(metrics))
            print('=' * 80)

        results = []
        for depth in range(min_depth, max_depth+1):
            # Select all directories of current depth
            self.conn.execute(f"select path from index where depth = {depth} "
                              f"and is_dir = 1 and path like "
                              f"'{top_directory}%';")
            subdirectories = [dn[0] for dn in self.conn.fetchall()]
            logging.debug(f'Found {len(subdirectories)} directories at level '
                          f'{depth}')
            for basedir in subdirectories:
                # Root directory has to be treated in a special way
                if basedir == '.':
                    pattern = '%'
                else:
                    pattern = f"{basedir}%"

                sizes = self.query_metrics(
                        metrics, pattern, older_than=older_than,
                        newer_than=newer_than, timestamp_type=timestamp_type,
                    )
                results.append([basedir, 'ALL', depth] + sizes)

                # Print usage for each user separately
                if per_user:
                    self.conn.execute("select distinct uid from index where "
                                      f"path like '{pattern}';")
                    uids = [uid[0] for uid in self.conn.fetchall()]
                    uids_str = ",".join([f'{uid}' for uid in uids])
                    logging.debug(f'Users with files inside {basedir}: '
                                  f'{uids_str}')
                    for uid in uids:
                        sizes = self.query_metrics(
                            metrics, pattern, older_than=older_than,
                            newer_than=newer_than, uid=uid,
                            timestamp_type=timestamp_type
                        )
                        username = pwd.getpwuid(uid).pw_name
                        results.append([basedir, username, depth] + sizes)
        if not suppress_output:
            suffixes = ["" if m == "inodes" else "B" for m in metrics]
            columns = ['path', 'user', 'depth'] + metrics

            # Some sanity checks befor sorting
            if 'user' in sort_by:
                assert per_user, ('When sorting by user, '
                                  'per_user has to be True')
                assert min_depth == 0, \
                    ('When sorting by user, min_depth has to be 0')
                assert min_depth == max_depth, \
                    ('When sorting by user, only a single depth can be '
                     'considered, so min_depth has to be equal to max_depth')
            elif per_user:
                assert 'depth' in sort_by, \
                    'When reporting per user, sorting has to include depth'

            # Sort the obtained results
            results = self.sort_list(results, columns, sort_by)

            for res in results:
                basedir = res[0]
                username = res[1]
                depth = res[2]
                sizes = res[3:]
                print_usage_single(
                    sizes, basedir if username == 'ALL' else username,
                    human_readable=human_readable, si_units=si_units,
                    prefix=" " * (0 if username == 'ALL' else 4),
                    suffixes=suffixes
                )
                if per_user and username == 'ALL':
                    print('-' * 80)

        return results

    def sort_list(self, results, columns, sortby):
        """
        Sorts a list of result tuples based on specified columns and sort
        order.

        Parameters:
        - results (list): The list of result tuples to be sorted.
        - columns (list): The list of column names corresponding to the values
          in each tuple.
        - sortby (list): The list of column names to sort by, in order of
          priority.

        Returns:
        - list: A new list sorted based on the specified columns and order.

        Raises:
        - AssertionError: If a column in sortby is not found in columns.
        """

        class Sorter:
            """A helper class to define sorting logic based on column indices
               and comparator classes."""

            def __init__(self, columns, sortby):
                self.keys = []
                for ikey, key in enumerate(sortby):
                    # Ensure the sort key exists in the columns
                    assert key in columns, \
                        (f"Impossible to sort by {key}, "
                         f"it is not in the columns {columns}")

                    # Determine the comparator class based on the column type
                    if key in ['size', 'inodes']:
                        # Reverse sorting for 'size' and 'inodes'
                        self.keys.append((columns.index(key), reversor))
                    else:
                        # Normal sorting for other columns
                        self.keys.append((columns.index(key), versor))

            def sort(self, res):
                """Creates a list of comparator objects for sorting."""
                return [cls(res[ikey]) for ikey, cls in self.keys]

        class comparator:
            """Base comparator class to define equality comparison based on
            the wrapped object."""

            def __init__(self, obj):
                self.obj = obj

            def __eq__(self, other):
                return other.obj == self.obj

        class reversor(comparator):
            """Comparator class for reverse sorting."""
            def __lt__(self, other):
                return other.obj < self.obj  # Reversed comparison logic

        class versor(comparator):
            """Comparator class for normal sorting."""
            def __lt__(self, other):
                return other.obj > self.obj  # Normal comparison logic

        # Instantiate a Sorter object with the specified columns and sort order
        sorter = Sorter(columns, sortby)

        # Sort the list using the Sorter object's sort method as the key
        sorted_list = sorted(results, key=sorter.sort)
        return sorted_list

    def query_metrics(self, metrics, pattern, older_than=None,
                      newer_than=None, uid=None, timestamp_type=None):
        sizes = []
        for metric in metrics:
            if metric == 'size':
                qmetric = 'sum'
            elif metric == 'inodes':
                qmetric = 'count'
            else:
                raise NotImplementedError(f'Unknown metric {metric}')

            # Print usage for this directory
            query = f"select {qmetric}(size) from index where path " \
                    f"like '{pattern}'"

            # Filter based on timestamp
            if older_than:
                query += f" and {timestamp_type} < '{older_than}'"
            if newer_than:
                query += f" and {timestamp_type} > '{newer_than}'"

            # Filter based on user
            if uid is not None:
                query += f' and uid = {uid};'

            # Get value
            self.conn.execute(query)
            sizes.append(self.conn.fetchone()[0])
        return sizes
