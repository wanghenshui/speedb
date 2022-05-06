import os
import re
import sys
from enum import Enum, auto
from dataclasses import dataclass
import difflib

cmdline_args = ""


class TestResult(Enum):
    PASS = auto()
    FAIL = auto()
    DISABLED = auto()
    EMPTY = auto()
    ASSERTION = auto()
    SEG_FAULT = auto()
    SIG_ABORT = auto()
    OTHER = auto()
    INVALID = auto()

    def __repr__(self):
        if self == TestResult.INVALID:
            return ""
        else:
            return self.name


class MismatchType(Enum):
    FILE_MISSING_IN_NEW = auto()
    TEST_MISSING_IN_NEW = auto()
    FILE_MISSING_IN_REF = auto()
    TEST_MISSING_IN_REF = auto()
    MISMATCHING_RESULTS = auto()
    MISMATCHING_LOGS = auto()
    NEW_TEST_TOOK_TOO_LONG = auto()
    NEW_TEST_ITER_TOOK_TOO_LONG = auto()
    TEST_NO_ITERS_IN_REF_WITH_ITERS_IN_NEW = auto()
    TEST_WITH_ITERS_IN_REF_NO_ITERS_IN_NEW = auto()
    ITERS_MISSING_IN_REF = auto()
    ITERS_MISSING_IN_NEW = auto()
    MISMATCHING_NUM_OF_ITERS = auto()
    MISMATCHING_ITER_PARAMETERS = auto()


# Unified Logs Meta-Data
@dataclass
class LogsMd:
    date: str = None
    folder: str = None
    description: str = None

    DATE_FIELD_TEXT = "Date:"
    FOLDER_FIELD_TEXT = "Folder:"
    DESCRIPTION_FIELD_TEXT = "Description:"


@dataclass
class TestLogNameInfo:
    log_file_name: str
    test_file_name: str
    test_name: str

    def __lt__(self, other):
        if self.test_file_name == other.test_file_name:
            return self.test_name < other.test_name
        else:
            return self.test_file_name < other.test_file_name


@dataclass
class TestLogInfo:
    name_info: TestLogNameInfo
    log_file_contents: str

    def __lt__(self, other):
        return self.name_info < other.name_info


@dataclass
class TestHeader:
    name_info: TestLogNameInfo
    iter_num: int
    disabled: bool


@dataclass
class TestInfo:
    header: TestHeader
    result: TestResult
    time_ms: int
    line_in_unified_log: int
    log: str
    iter_params: str = None

    def __str__(self):
        "Test Info:" + "log_file_name:" +\
            self.header.name_info.log_file_name + ", " +\
            "line:" + self.line_in_unified_log + ", " +\
            "result:" + self.result + ", " +\
            "time_ms:" + self.time_ms + ", " +\
            "iter_num:" + self.header.iter_num


@dataclass
class ItersGroup:
    tests: dict

    def get_first_test(self):
        return list(self.tests.items())[0][1]

    def get_test_by_iter_num(self, iter_num):
        return self.tests[str(iter_num)]

    def get_iters_num_list(self):
        return [int(iter_num_str) for iter_num_str in list(self.tests.keys())]

    def get_tests_list(self):
        return list(self.tests.values())

    def get_times_ms_list(self):
        return [test_info.time_ms for test_info in list(self.tests.values())]

    def get_min_time_ms(self):
        return min(self.get_times_ms_list())

    def get_max_time_ms(self):
        return max(self.get_times_ms_list())


@dataclass
class DiffInfo:
    test_file_name: str
    test_name: str
    mismatch_type: MismatchType
    ref_result: TestResult
    ref_test_log_file_name: str
    ref_unified_log_line_num: int
    new_result: TestResult
    new_test_log_file_name: str
    new_unified_log_line_num: int
    iterations: list = None
    ref_iter_params: str = None
    new_iter_params: str = None
    misc1: any = None
    misc2: any = None
    misc3: any = None

    def __lt__(self, other):
        if self.test_file_name != other.test_file_name:
            return self.test_file_name < other.test_file_name
        if self.test_name != other.test_name:
            return self.test_name < other.test_name

        return True

    @staticmethod
    def get_fields_as_csv_header():
        return ["test file name",
                "test name",
                "Mismatch Type",
                "Ref Result",
                "New Result",
                "Iteration(s)",
                "Ref Line-Num",
                "New Line-Num",
                "Misc1",
                "Misc2",
                "Misc3"]

    def get_iters_for_csv(self):
        iterations = ""

        if self.iterations:
            if type(self.iterations) is list:
                iterations_as_int_list = [int(iter_num)
                                          for iter_num in self.iterations]

                # If iterations is a contiguous range, display
                # as "<min> - <max>"
                if len(iterations_as_int_list) > 2 and\
                        sorted(iterations_as_int_list) == \
                        list(range(min(iterations_as_int_list),
                                   max(iterations_as_int_list) + 1)):
                    iterations = f"{min(iterations_as_int_list)} - " \
                                 f"{max(iterations_as_int_list)}"
                else:
                    iterations = ",".join([str(i) for i in self.iterations])
            elif self.iterations != NO_ITER_VALUE:
                iterations = str(self.iterations)

        return iterations

    def get_as_csv_row(self):
        test_file_name = self.test_file_name
        test_name = self.test_name if self.test_name else ""
        mismatch_type = self.mismatch_type.name
        ref_result = self.ref_result.name if self.ref_result else ""
        new_result = self.new_result.name if self.new_result else ""
        iterations = self.get_iters_for_csv()
        ref_line_num = self.ref_unified_log_line_num if\
            self.ref_unified_log_line_num else ""
        new_line_num = self.new_unified_log_line_num if\
            self.new_unified_log_line_num else ""
        misc1 = self.misc1 if self.misc1 else ""
        misc2 = self.misc2 if self.misc2 else ""
        misc3 = self.misc3 if self.misc3 else ""

        csv_row = [
            test_file_name,
            test_name,
            mismatch_type,
            ref_result,
            new_result,
            iterations,
            ref_line_num,
            new_line_num,
            misc1,
            misc2,
            misc3]

        return csv_row


@dataclass
class ExceptionInfo:
    e: Exception
    exception_type: any
    exception_object: any
    exception_traceback: any
    file_name: str
    line_number: int

    def __str__(self):
        return f"{self.exception_type} ({self.file_name} [{self.line_number}])"


@dataclass
class ErrorInfo:
    description: str
    test_file_name: str = None
    test_name: str = None
    exc_info: ExceptionInfo = None

    def __str__(self):
        return f"{self.description} - test_file_name:{self.test_file_name} " \
               f"[{self.test_name}]\n{self.exc_info}\n"


# A dummy value to indicate an invalid iteration number
NO_ITER_VALUE = 2 ** 20
MIN_REF_TEST_TIME_MS_TO_CHECK_NEW_TIME = 100
MAX_ALLOWED_TIME_INCREASE_PERCENT = 30

# Record the script's exit code in case an error occurred but
# the script continues to run
global exit_code
exit_code = 0


class UnifiedConsts:
    # A delimiter line for the meta-data section at the top of
    # the unified log file
    MD_DELIM_LINE = '='*100

    # A delimiter line for the lines of a single test log
    # within the unified file
    TEST_DELIM_LINE = "<" + "-" * 100 + ">"

    NUM_TEST_HEADER_LINES = 4

    LOG_FILE_NAME_TITLE = "LOG FILE NAME:"
    FILE_TITLE = "FILE:"
    TEST_TITLE = "TEST:"
    DISABLED_TITLE = "DISABLED:"


def normalize_test_log_seg_fault(test_lines):
    return re.sub(r"Received signal 11 \(Segmentation fault\).*",
                  "Received signal 11 (Segmentation fault)", test_lines,
                  flags=re.MULTILINE | re.DOTALL)


def normalize_test_log_sig_abort(test_lines):
    return re.sub(r"Received signal 6 \(Aborted\).*",
                  "Received signal 6 (Aborted)", test_lines,
                  flags=re.MULTILINE | re.DOTALL)


def normalize_iter_info_in_test_log(test_name, iter_num, log_line_with_iter):
    if not iter_num:
        return log_line_with_iter

    normalized_log1 = re.sub(r"\.DISABLED_", ".", log_line_with_iter,
                             flags=re.MULTILINE)

    # Replace iter_num with "*"
    normalized_log2 = normalized_log1.replace(f"{test_name}/{iter_num}",
                                              f"{test_name}/*")

    # Replace parameters with "*"
    normalized_log3 = re.sub(r"GetParam\(\) =(.*)", "GetParam() = *",
                             normalized_log2)

    return normalized_log3


def normalize_test_log_with_iter(test_name, test_result, iter_num,
                                 test_log_with_iter):
    normalized_log = normalize_iter_info_in_test_log(test_name, iter_num,
                                                     test_log_with_iter)

    if test_result == TestResult.SEG_FAULT.name:
        normalized_log = normalize_test_log_seg_fault(normalized_log)
    elif test_result == TestResult.SIG_ABORT.name:
        normalized_log = normalize_test_log_sig_abort(normalized_log)

    # Replace all time values
    return re.sub(r"[0-9]+ ms", "ms", normalized_log)


def get_diff_lines(s1, s2):
    diff_lines = ""
    for line in difflib.ndiff(s1.splitlines(), s2.splitlines()):
        if line[0] == '-' or line[0] == '+':
            diff_lines += line + "\n"
    return diff_lines


def are_test_logs_with_iter_equal(test_name, test_info1, test_info2):
    if test_info1.result != test_info2.result:
        return False

    normalized_log1 = normalize_test_log_with_iter(test_name,
                                                   test_info1.result,
                                                   test_info1.header.iter_num,
                                                   test_info1.log)
    normalized_log2 = normalize_test_log_with_iter(test_name,
                                                   test_info2.result,
                                                   test_info2.header.iter_num,
                                                   test_info2.log)

    if normalized_log1 == normalized_log2:
        return True, ""
    else:
        return False, get_diff_lines(normalized_log1, normalized_log2)


def build_exception_info(e):
    exception_type, exception_object, exception_traceback = sys.exc_info()
    file_name = os.path.split(
        exception_traceback.tb_frame.f_code.co_filename)[1]
    line_number = exception_traceback.tb_lineno

    return ExceptionInfo(e=e,
                         exception_type=exception_type,
                         exception_object=exception_object,
                         exception_traceback=exception_traceback,
                         file_name=file_name,
                         line_number=line_number)


def report_exception(message, set_script_exit_code):
    exception_type, exception_object, exception_traceback = sys.exc_info()
    file_name = os.path.split(
        exception_traceback.tb_frame.f_code.co_filename)[1]
    line_number = exception_traceback.tb_lineno

    print(f"\n{'!'*150}\n"
          f"{message}\n"
          f"Exception Occurred: {exception_type}, "
          f"File[Line]:{file_name}[{line_number}]"
          f"\n{'!'*150}\n")

    if set_script_exit_code:
        global exit_code
        exit_code = 1


def report_processing_errors(description, processing_errors,
                             set_script_exit_code):
    print(f"{'!'*150}")
    print(f"{description}\n")

    for error in processing_errors:
        print(f"{error}")

    print(f"{'!'*150}")

    if set_script_exit_code:
        global exit_code
        exit_code = 1


def get_exit_code():
    global exit_code
    return exit_code


def percentage_diff_str(ref, new):
    percentage = 100 * float(new - ref)/float(ref)
    return str(int(round(percentage, 0))) + "%"


# =============================================================================
def add_cmdline_arg_ref_log(parser):
    parser.add_argument("ref_log_full_name",
                        metavar="[ref log]",
                        help="Reference log file name")


def add_cmdline_arg_new_log(parser):
    parser.add_argument("new_log_full_name",
                        metavar="[new log]",
                        help="Reference log file name")


def add_cmdline_arg_csv(parser):
    parser.add_argument("diff_csv_full_name",
                        metavar="[csv file]",
                        help="csv file name to which the result will be "
                             "written")


def add_cmdline_optional_arg_description(parser):
    parser.add_argument("-d", "--description",
                        help="Description of the branch/version collected",
                        default="")


def add_cmdline_optional_arg_report_pass(parser):
    parser.add_argument("-p", "--report-pass",
                        help="Include in the csv information about tests "
                             "that have passed",
                        action="store_true",
                        default=False)


def add_cmdline_optional_arg_report_full_diff(parser):
    parser.add_argument("-f", "--report-full-diff",
                        help="Include in the csv detailed diff information "
                             "about mismatching logs",
                        action="store_true",
                        default=False)


def add_cmdline_optional_arg_ignore_too_long(parser):
    parser.add_argument("-t", "--ignore-too-long",
                        help="Ignore too long tests (conflicts with -m / -i)",
                        action="store_true",
                        default=False)


def add_cmdline_optional_arg_min_time(parser):
    parser.add_argument("-m", "--min-ref-time",
                        help="Min test time (ms) to check if too long ("
                             "conflicts with -t)",
                        type=int,
                        default=MIN_REF_TEST_TIME_MS_TO_CHECK_NEW_TIME)


def add_cmdline_optional_arg_min_ref_time_increase(parser):
    parser.add_argument("-i", "--ref-time-increase",
                        help="Min increase in ref test time (percent) to "
                             "report as too long (conflicts with -t)",
                        type=int,
                        default=MAX_ALLOWED_TIME_INCREASE_PERCENT)


def check_cmdline_conflicts():
    if cmdline_args.ignore_too_long:
        if ("-m" in sys.argv or "--min-ref-time" in sys.argv or
                "-i" in sys.argv or "--ref-time-increase" in sys.argv):
            print("Error: -t conflicts with -m and with -i")
            return False
        else:
            return True
    else:
        return True
