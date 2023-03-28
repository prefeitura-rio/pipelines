# flake8: noqa: E501
import datetime
import time
import warnings
from typing import Any, List, Optional, Tuple

import h5py
import numpy as np


def print_error(
    message: str = "ERROR",
    verbose: bool = True,
    skip_line_before: bool = True,
    skip_line_after: bool = True,
    bold: bool = False,
) -> None:
    """Print message in red."""
    if verbose:
        string_before = "\n" if skip_line_before else ""
        string_after = "\n" if skip_line_after else ""
        if bold:
            print(f"{string_before}\x1b[1;30;41m[ {message} ]\x1b[0m{string_after}")
        else:
            print(f"{string_before}\x1b[31m{message}\x1b[0m{string_after}")


def print_warning(
    message: str = "WARNING",
    verbose: bool = True,
    skip_line_before: bool = True,
    skip_line_after: bool = True,
    bold: bool = False,
) -> None:
    """Print message in yellow."""
    if verbose:
        string_before = "\n" if skip_line_before else ""
        string_after = "\n" if skip_line_after else ""
        if bold:
            print(f"{string_before}\x1b[1;30;43m[ {message} ]\x1b[0m{string_after}")
        else:
            print(f"{string_before}\x1b[33m{message}\x1b[0m{string_after}")


def print_ok(
    message: str = "OK",
    verbose: bool = True,
    skip_line_before: bool = True,
    skip_line_after: bool = True,
    bold: bool = False,
) -> None:
    """Print message in green."""
    if verbose:
        string_before = "\n" if skip_line_before else ""
        string_after = "\n" if skip_line_after else ""
        if bold:
            print(f"{string_before}\x1b[1;30;42m[ {message} ]\x1b[0m{string_after}")
        else:
            print(f"{string_before}\x1b[32m{message}\x1b[0m{string_after}")


def print_info(
    message: str, verbose: bool = True, skip_line_before: bool = False, skip_line_after: bool = False
) -> None:
    if verbose:
        string_before = "\n" if skip_line_before else ""
        string_after = "\n" if skip_line_after else ""
        print(f"{string_before}{message}{string_after}")


def parse_dates_argument(dates_str: str, format: str = "%Y%m%d") -> list:
    pre_dates = dates_str.split(",")
    dates = []
    for pre_date in pre_dates:
        if "-" in pre_date:
            date_range = pre_date.split("-")
            if len(date_range) != 2:
                raise Exception("Error: wrong formatting for dates")
            start = datetime.datetime.strptime(date_range[0], "%Y%m%d").date()
            end = datetime.datetime.strptime(date_range[1], "%Y%m%d").date()
            delta = end - start
            dates_between = [(start + datetime.timedelta(days=i)).strftime(format) for i in range(delta.days + 1)]
            dates = list(set(dates).union(set(dates_between)))

        else:
            try:
                new_pre_date = datetime.datetime.strptime(pre_date, "%Y%m%d").strftime(format)
            except ValueError:
                raise Exception("Error: wrong formatting for dates")

            if pre_date not in dates:
                dates.append(new_pre_date)
    return sorted(dates)


def is_strictly_increasing(lst):
    stack = []
    for i in lst:
        if stack and i <= stack[-1]:
            return False
        stack.append(i)
    return True


class _TicToc(object):
    """
    Author: Hector Sanchez
    Date: 2018-07-26
    Description: Class that allows you to do 'tic toc' to your code.
    This class was based on https://github.com/hector-sab/ttictoc, which is
    distributed under the MIT license. It prints time information between
    successive tic() and toc() calls.
    Example:
        from src.utils.general_utils import tic,toc
        tic()
        tic()
        toc()
        toc()
    """

    def __init__(
        self,
        name: str = "",
        method: Any = "time",
        nested: bool = False,
        print_toc: bool = True,
    ) -> None:
        """
        Args:
            name (str): Just informative, not needed
            method (int|str|ftn|clss): Still trying to understand the default
                options. 'time' uses the 'real wold' clock, while the other
                two use the cpu clock. To use your own method,
                do it through this argument
                Valid int values:
                    0: time.time | 1: time.perf_counter | 2: time.proces_time
                    3: time.time_ns | 4: time.perf_counter_ns
                    5: time.proces_time_ns
                Valid str values:
                  'time': time.time | 'perf_counter': time.perf_counter
                  'process_time': time.proces_time | 'time_ns': time.time_ns
                  'perf_counter_ns': time.perf_counter_ns
                  'proces_time_ns': time.proces_time_ns
                Others:
                  Whatever you want to use as time.time
            nested (bool): Allows to do tic toc with nested with a
                single object. If True, you can put several tics using the
                same object, and each toc will correspond to the respective tic.
                If False, it will only register one single tic, and
                return the respective elapsed time of the future tocs.
            print_toc (bool): Indicates if the toc method will print
                the elapsed time or not.
        """
        self.name = name
        self.nested = nested
        self.tstart: Any[List, None] = None
        if self.nested:
            self.set_nested(True)

        self._print_toc = print_toc

        self._int2strl = [
            "time",
            "perf_counter",
            "process_time",
            "time_ns",
            "perf_counter_ns",
            "process_time_ns",
        ]
        self._str2fn = {
            "time": [time.time, "s"],
            "perf_counter": [time.perf_counter, "s"],
            "process_time": [time.process_time, "s"],
            "time_ns": [time.time_ns, "ns"],
            "perf_counter_ns": [time.perf_counter_ns, "ns"],
            "process_time_ns": [time.process_time_ns, "ns"],
        }

        if type(method) is not int and type(method) is not str:
            self._get_time = method

        if type(method) is int and method < len(self._int2strl):
            method = self._int2strl[method]
        elif type(method) is int and method > len(self._int2strl):
            method = "time"

        if type(method) is str and method in self._str2fn:
            self._get_time = self._str2fn[method][0]
            self._measure = self._str2fn[method][1]
        elif type(method) is str and method not in self._str2fn:
            self._get_time = self._str2fn["time"][0]
            self._measure = self._str2fn["time"][1]

    def _print_elapsed(self) -> None:
        """
        Prints the elapsed time
        """
        if self.name != "":
            name = "[{}] ".format(self.name)
        else:
            name = self.name
        print("-{0}elapsed time: {1:.3g} ({2})".format(name, self.elapsed, self._measure))

    def tic(self) -> None:
        """
        Defines the start of the timing.
        """
        if self.nested:
            self.tstart.append(self._get_time())
        else:
            self.tstart = self._get_time()

    def toc(self, print_elapsed: Optional[bool] = None) -> None:
        """
        Defines the end of the timing.
        """
        self.tend = self._get_time()
        if self.nested:
            if len(self.tstart) > 0:
                self.elapsed = self.tend - self.tstart.pop()
            else:
                self.elapsed = None
        else:
            if self.tstart:
                self.elapsed = self.tend - self.tstart
            else:
                self.elapsed = None

        if print_elapsed is None:
            if self._print_toc:
                self._print_elapsed()
        else:
            if print_elapsed:
                self._print_elapsed()

        # return(self.elapsed)

    def set_print_toc(self, set_print: bool) -> None:
        """
        Indicate if you want the timed time printed out or not.
        Args:
          set_print (bool): If True, a message with the elapsed time
            will be printed.
        """
        if type(set_print) is bool:
            self._print_toc = set_print
        else:
            warnings.warn(
                "Parameter 'set_print' not boolean. " "Ignoring the command.",
                Warning,
            )

    def set_nested(self, nested: bool) -> None:
        """
        Sets the nested functionality.
        """
        # Assert that the input is a boolean
        if type(nested) is bool:
            # Check if the request is actually changing the
            # behaviour of the nested tictoc
            if nested != self.nested:
                self.nested = nested

                if self.nested:
                    self.tstart = []
                else:
                    self.tstart = None
        else:
            warnings.warn(
                "Parameter 'nested' not boolean. " "Ignoring the command.",
                Warning,
            )


class TicToc(_TicToc):
    def tic(self, nested: bool = True) -> None:
        """
        Defines the start of the timing.
        """
        if nested:
            self.set_nested(True)

        if self.nested:
            self.tstart.append(self._get_time())
        else:
            self.tstart = self._get_time()


__TICTOC_8320947502983745 = TicToc()
tic = __TICTOC_8320947502983745.tic
toc = __TICTOC_8320947502983745.toc


def read_attributes(hval):
    attr = {}
    for k in hval.attrs:
        attr[k] = hval.attrs[k]
    return attr


# returns summary of group.
# the only element for comparison here is the group's attributes
def read_group(hval):
    desc = {}
    desc["attr"] = read_attributes(hval)
    desc["htype"] = "group"
    return desc


# returns summary of dataset
# the only elements for comparison here are the dataset's attributes,
#   and the dataset
def read_data(hval):
    desc = {}
    desc["attr"] = read_attributes(hval)
    desc["htype"] = "dataset"
    desc["data"] = np.array(hval)
    return desc


# creates and returns a summary description for every element in a group
def evaluate_group(path, grp):
    desc = {}
    for k, v in grp.items():
        if isinstance(v, h5py.Dataset):
            desc[k] = read_data(v)
        elif isinstance(v, h5py.Group):
            desc[k] = read_group(v)
        else:
            raise Exception(f"Unknown h5py type: {type(v)} ({path} --  {k})")
    return desc


def diff_groups(file1, grp1, file2, grp2, path):
    desc1 = evaluate_group(path, grp1)
    desc2 = evaluate_group(path, grp2)

    diff = []

    keys1 = set(desc1.keys())
    keys2 = set(desc2.keys())

    diff_keys1 = keys1.difference(keys2)
    diff_keys2 = keys2.difference(keys1)

    if len(diff_keys1):
        diff.append(f"{path}: Elements {diff_keys1} only in '{file1}' (DIFF_UNIQUE_A)")

    if len(diff_keys2):
        diff.append(f"{path}: Elements {diff_keys2} only in '{file2}' (DIFF_UNIQUE_B)")
    common = keys1.intersection(keys2)

    if len(common) == 0:
        return diff

    for name in common:
        path_to_element = path + name

        # compare types
        h1 = desc1[name]["htype"]
        h2 = desc2[name]["htype"]
        if h1 != h2:
            diff.append(f"{path_to_element}: Different element types: '{h1}' and '{h2}' (DIFF_OBJECTS)")
            continue  # different hdf5 types -- don't try to compare further
        # compare attributes
        keys_attrs1 = set(desc1[name]["attr"].keys())
        keys_attrs2 = set(desc2[name]["attr"].keys())

        diff_keys1 = keys_attrs1.difference(keys_attrs2)
        diff_keys2 = keys_attrs2.difference(keys_attrs1)

        if len(diff_keys1):
            diff.append(f"{path_to_element}: Attributes {diff_keys1} only in '{file1}' (DIFF_UNIQ_ATTR_A)")
        if len(diff_keys2):
            diff.append(f"{path_to_element}: Attributes {diff_keys2} only in '{file2}' (DIFF_UNIQ_ATTR_B)")
        common_keys_attr = keys_attrs1.intersection(keys_attrs2)
        for k in common_keys_attr:
            v1 = desc1[name]["attr"][k]
            v2 = desc2[name]["attr"][k]
            try:
                if v1 != v2:
                    diff.append(
                        f"{path_to_element}: Attribute '{k}' has different values: '{v1}' and '{v2}' (DIFF_ATTR)"
                    )
            except ValueError:
                if not np.array_equal(v1, v2):
                    diff.append(
                        f"{path_to_element}: Attribute '{k}' has different values (and are numpy ndarrays) (DIFF_ATTR)"
                    )
        # handle datasets first
        if h1 != "dataset":
            continue
        # compare data
        data1 = desc1[name]["data"]
        data2 = desc2[name]["data"]
        if type(data1) != type(data2):
            diff.append(f"{path_to_element}: Different dtypes: '{type(data1)}' and '{type(data2)}' (DIFF_DTYPE)")
        if not np.array_equal(data1, data2):
            diff.append(f"{path_to_element}: Different datasets (DIFF_DATASET)")
    for name in common:
        # compare types
        if desc1[name]["htype"] != desc2[name]["htype"]:
            continue  # problem already reported
        if desc1[name]["htype"] != "group":
            continue
        # recurse into subgroup
        diff.extend(diff_groups(file1, grp1[name], file2, grp2[name], path + name + "/"))

    return diff


def diff_hdf_files(file1, file2):
    try:
        hdf1 = h5py.File(file1, "r")
    except IOError:
        print(f"Unable to open file {file1}")
        exit()
    try:
        hdf2 = h5py.File(file2, "r")
    except IOError:
        print(f"Unable to open file {file2}")
        exit()
    return diff_groups(file1, hdf1["/"], file2, hdf2["/"], "/")
