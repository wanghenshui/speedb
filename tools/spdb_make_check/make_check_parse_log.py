import re
import csv
import argparse

import make_check_utils as mcu
from make_check_utils import TestLogNameInfo, UnifiedConsts, \
    report_exception, NO_ITER_VALUE, get_exit_code, TestResult, \
    ItersGroup, TestHeader, are_test_logs_with_iter_equal, \
    TestInfo, LogsMd, \
    add_cmdline_arg_csv,\
    add_cmdline_optional_arg_report_pass


COMMENT_LINE_REGEX = r"############.*############"


# Parses the test name and the optional iter num from the test
# name header line Returns the test name and the iter number
# (if available) or NO_ITER_VALUE if not
def get_test_name_and_iter_from_test_name_log_line(log_lines, line_num):
    # Extract the test name + iter part of the line
    test_name_and_iter_match = \
        re.findall(fr"{UnifiedConsts.TEST_TITLE} (.*)", log_lines[line_num])
    assert len(test_name_and_iter_match) == 1,\
        "Failed to match a single test name."
    test_name_and_iter = test_name_and_iter_match[0].strip()

    test_name = test_name_and_iter
    iter_num = NO_ITER_VALUE

    parsed_test_name_and_iter_match = \
        re.findall(r"(.*)(\/[0-9]+)$", test_name_and_iter)
    if parsed_test_name_and_iter_match:
        test_name = parsed_test_name_and_iter_match[0][0].strip()
        iter_num = parsed_test_name_and_iter_match[0][1].strip()[1:]

    return test_name, iter_num


def get_test_result_from_test_log(test_name, test_log):
    if re.findall(r"Segmentation fault", test_log, re.MULTILINE):
        return TestResult.SEG_FAULT
    elif re.findall(r"Assertion", test_log, re.MULTILINE):
        return TestResult.ASSERTION
    if re.findall(r"Received signal 6 \(Aborted\)", test_log, re.MULTILINE):
        return TestResult.SIG_ABORT
    elif re.findall(r"\[  FAILED  \]", test_log, re.MULTILINE):
        return TestResult.FAIL
    elif re.findall(r"YOU HAVE [0-9]+ DISABLED TEST", test_log, re.MULTILINE):
        return TestResult.DISABLED
    elif re.findall(r"\[==========\] 0 tests from 0 test cases ran",
                    test_log, re.MULTILINE):
        return TestResult.EMPTY
    elif re.findall(r"\[  PASSED  \] 1 test", test_log, re.MULTILINE):
        return TestResult.PASS
    elif re.findall(rf"\[       OK \] {test_name}", test_log, re.MULTILINE):
        return TestResult.PASS
    else:
        return TestResult.OTHER


def get_test_time_ms_from_test_log(test_log):
    test_time_match = re.findall(r"\[.*\].*\(([0-9]+) ms( total)?\)",
                                 test_log, re.MULTILINE)
    if test_time_match:
        return int(test_time_match[0][0].strip())
    else:
        return -1


def get_test_iter_params(test_result, test_log):
    iter_params_match = re.findall(r"\[.*\].*GetParam\(\) = (\(.*\))[ $]",
                                   test_log, re.MULTILINE)
    if iter_params_match:
        return iter_params_match[0]
    else:
        return None


def parse_test_header(log_lines, line_num):
    num_lines = len(log_lines)
    assert line_num + UnifiedConsts.NUM_TEST_HEADER_LINES < num_lines,\
        f"Not Enough Lines in Log. line_num={line_num}, num lines:{num_lines}"

    # Log File Name
    log_file_name_match = \
        re.findall(fr"{UnifiedConsts.LOG_FILE_NAME_TITLE} (.*)",
                   log_lines[line_num])
    assert len(log_file_name_match) == 1,\
        f"Failed to match a single log file name \nLine " \
        f"[{line_num}]:|{log_lines[line_num]}|"
    log_file_name = log_file_name_match[0]

    # Test File Name
    line_num += 1
    test_file_name_match = re.findall(fr"{UnifiedConsts.FILE_TITLE} (.*)",
                                      log_lines[line_num])
    assert len(test_file_name_match) == 1,\
        f"Failed to match a single test file name \nLine [{line_num}]:" \
        f"|{log_lines[line_num]}|"
    test_file_name = test_file_name_match[0].strip()

    # Test Name & (Optional) Iter
    line_num += 1
    test_name, iter_num = \
        get_test_name_and_iter_from_test_name_log_line(log_lines, line_num)

    # Disabling Information
    line_num += 1
    disabled_match = re.findall(fr"{UnifiedConsts.DISABLED_TITLE} (.*)",
                                log_lines[line_num])
    assert len(disabled_match) == 1,\
        f"Failed to match a test disabling value. Line [{line_num}]:" \
        f"|{log_lines[line_num]}|"
    disabled_test = True if disabled_match[0].strip() == "YES" else False

    line_num += 1
    test_name_info = TestLogNameInfo(log_file_name=log_file_name,
                                     test_file_name=test_file_name,
                                     test_name=test_name)
    header = TestHeader(name_info=test_name_info,
                        iter_num=iter_num,
                        disabled=disabled_test)
    return line_num, header


def group_logically_equivalent_test_iters(test_name, test_iters_dict):
    groups_list = list()

    for iter_num, checked_test_info in test_iters_dict.items():
        assert iter_num == checked_test_info.header.iter_num,\
            f"Mismatching iter_num ({iter_num}) and iter-num in test_info " \
            f"({checked_test_info.header.iter_num})"

        was_unified = False

        # go over all existing groups (each group is a dict of iters in
        # that group) and see if the checked iter
        # may be added to this group
        for group_idx, iters_group in enumerate(groups_list):
            first_group_test_info = iters_group.get_first_test()
            if are_test_logs_with_iter_equal(test_name,
                                             first_group_test_info,
                                             checked_test_info):
                was_unified = True
                groups_list[group_idx].tests[str(iter_num)] = checked_test_info
                break

        if not was_unified:
            groups_list.append(
                ItersGroup(tests={str(iter_num): checked_test_info}))

    return groups_list


def unify_equal_logs(parsed_log_dict):
    unified_logs = dict()

    for test_file_name, file_tests_dict in parsed_log_dict.items():
        if test_file_name not in unified_logs.keys():
            unified_logs[test_file_name] = dict()

            for test_name, test_iters_info in file_tests_dict.items():
                if (len(test_iters_info) > 1) or \
                        (len(test_iters_info) == 1 and NO_ITER_VALUE not in
                         test_iters_info.keys()):
                    # The test has multiple iterations => try to unify them
                    unified_logs[test_file_name][test_name] = \
                        group_logically_equivalent_test_iters(test_name,
                                                              test_iters_info)
                else:
                    # The test has not iterations (a dictionary with a single
                    # element mapped to the TestInfo)
                    assert len(test_iters_info.items()) == 1,\
                        f"Unexpected length ({len(test_iters_info.items())})" \
                        f" of test_iters_info"

                    for test_iter, test_info in test_iters_info.items():
                        assert type(test_info) is TestInfo
                        unified_logs[test_file_name][test_name] = test_info

    return unified_logs


# Parses the start of the unified log for the global meta-data
# Returns the number of lines processed and the parsed metadata
# (as an LogsMd object)
def parse_logs_md(log_lines, line_num):
    logs_md = LogsMd()
    num_lines = len(log_lines)

    found_md_start = False

    while line_num < num_lines:
        # Try to match the line as a meta-data section delimiter line
        md_start_delim_match = re.findall(rf"({UnifiedConsts.MD_DELIM_LINE})$",
                                          log_lines[line_num])
        line_num += 1

        if md_start_delim_match:
            found_md_start = True

            date_match = re.findall(fr"{LogsMd.DATE_FIELD_TEXT}(.+)$",
                                    log_lines[line_num])
            line_num += 1
            assert date_match, "Failed matching global file's Date field"
            logs_md.date = date_match[0].strip()

            folder_match = re.findall(fr"{LogsMd.FOLDER_FIELD_TEXT}(.+)$",
                                      log_lines[line_num])
            line_num += 1
            assert folder_match, "Failed matching global file's Folder field"
            logs_md.folder = folder_match[0].strip()

            description_match = re.findall(
                fr"{LogsMd.DESCRIPTION_FIELD_TEXT}(.*)$", log_lines[line_num])
            if description_match:
                logs_md.description = description_match[0].strip()
                line_num += 1
            else:
                logs_md.description = ""

            md_end_delim_match = \
                re.findall(rf"({UnifiedConsts.MD_DELIM_LINE})$",
                           log_lines[line_num])
            assert md_end_delim_match, \
                "Failed finding global file's md end section line"
            line_num += 1

            break

    assert found_md_start, \
        "Failed finding global file's Meta-data start section"

    return line_num, logs_md


# reads the log lines, starting at line_num and stop when finding the
# first test delimiting line
#
# Returns the result (found / not found) and the line number of
# the delimiting line
#
def find_next_test_delim_line(log_lines, line_num):
    num_lines = len(log_lines)

    while line_num < num_lines:
        test_delim_line_match = \
            re.findall(fr"{UnifiedConsts.TEST_DELIM_LINE}",
                       log_lines[line_num])
        assert len(test_delim_line_match) <= 1,\
            "Found more than one test delimiting string in a " \
            "single log line.Line [{line_num}]:|{log_lines[line_num]}|"

        if test_delim_line_match:
            break

        line_num += 1

    return line_num


def parse_unified_log(unified_log_full_file_name):
    tests_logs = dict()

    with open(unified_log_full_file_name, "r") as unified_log:
        log_lines = unified_log.readlines()
        log_lines = "".join(log_lines).strip().splitlines()
        num_lines = len(log_lines)

        line_num = 0
        line_num, logs_md = parse_logs_md(log_lines, line_num)

        print(f"Parsing {unified_log_full_file_name} (D"
              f"escription: {logs_md.description})")

        # Find the delimiter of the first test in the file
        line_num = find_next_test_delim_line(log_lines, line_num)

        while line_num+1 < num_lines:
            try:
                line_num += 1

                # Parse the test header
                test_header_line_num = line_num
                line_num, header = parse_test_header(log_lines,
                                                     test_header_line_num)

                # Find end test line and update line_num first
                # (before parsing), in case we assert in the code below
                test_start_line = line_num
                test_end_line_num = find_next_test_delim_line(log_lines,
                                                              test_start_line)
                test_log_lines_list = log_lines[test_start_line:
                                                test_end_line_num]
                # Skip comment lines in the log (allowed for debugging)
                test_log_lines_list = [line for line in test_log_lines_list
                                       if not re.findall(COMMENT_LINE_REGEX,
                                                         line)]
                test_log = "\n".join(test_log_lines_list)

                if not header.disabled:
                    test_result = \
                        get_test_result_from_test_log(
                            header.name_info.test_name,
                            test_log)
                    test_time_ms = get_test_time_ms_from_test_log(test_log)
                    iter_params = get_test_iter_params(test_result, test_log)
                else:
                    test_result = TestResult.DISABLED
                    test_time_ms = 0
                    iter_params = None

                test_info = \
                    TestInfo(header=header,
                             result=test_result,
                             time_ms=test_time_ms,
                             line_in_unified_log=test_header_line_num+1,
                             log=test_log,
                             iter_params=iter_params)

                test_file_name = header.name_info.test_file_name
                test_name = header.name_info.test_name
                iter_num = header.iter_num

                if test_file_name not in tests_logs.keys():
                    tests_logs[test_file_name] = dict()
                if test_name not in tests_logs[test_file_name].keys():
                    tests_logs[test_file_name][test_name] = dict()
                tests_logs[test_file_name][test_name][iter_num] = test_info
            except AssertionError as e:
                report_exception(f"Error parsing "
                                 f"{unified_log_full_file_name}\n"
                                 f"({e}) - "
                                 f"Log File Lines:[{test_start_line} - "
                                 f"{test_end_line_num}]",
                                 set_script_exit_code=True)
                raise

            line_num = test_end_line_num

    # 2nd pass - Unify all applicable tests that are deemed identical
    return logs_md, unify_equal_logs(tests_logs)


def write_parsed_log_csv_header(csv_writer):
    header = ["TEST FILE NAME",
              "TEST NAME",
              "Iters",
              "Params",
              "RESULT",
              "Min Time (ms)",
              "Max Time (ms)",
              "Line",
              "FILE"]
    csv_writer.writerow(header)


def write_iters_group_list_to_csv(csv_writer,
                                  test_file_name,
                                  test_name,
                                  iters_groups_list,
                                  report_passing_tests):
    for iters_group in iters_groups_list:
        assert type(iters_group) is ItersGroup

        # all iters in the group must have the same result
        # => just pick the first
        first_group_test_info = iters_group.get_first_test()

        if first_group_test_info.result != TestResult.PASS or \
                report_passing_tests:
            test_csv_data = \
                [test_file_name,
                 test_name,
                 ",".join(iters_group.tests.keys()),
                 first_group_test_info.iter_params,
                 first_group_test_info.result.name,
                 iters_group.get_min_time_ms(),
                 iters_group.get_max_time_ms(),
                 first_group_test_info.line_in_unified_log,
                 first_group_test_info.header.name_info.log_file_name]

            csv_writer.writerow(test_csv_data)


def write_test_info_to_csv(csv_writer,
                           test_file_name,
                           test_name,
                           test_info,
                           report_passing_tests):
    if test_info.result != TestResult.PASS or \
            report_passing_tests:
        test_iter = ""
        iter_params = ""
        test_csv_data = \
            [test_file_name,
             test_name,
             test_iter,
             iter_params,
             test_info.result.name,
             test_info.time_ms,
             test_info.time_ms,
             test_info.line_in_unified_log,
             test_info.header.name_info.log_file_name]
        csv_writer.writerow(test_csv_data)


def write_parsed_log_tests_to_csv(csv_writer,
                                  parsed_log_dict,
                                  report_passing_tests):
    for test_file_name, file_tests_dict in parsed_log_dict.items():
        for test_name, iters_groups_list in file_tests_dict.items():
            if type(iters_groups_list) is list:
                write_iters_group_list_to_csv(csv_writer,
                                              test_file_name,
                                              test_name,
                                              iters_groups_list,
                                              report_passing_tests)
            else:
                assert type(iters_groups_list) is TestInfo
                test_info = iters_groups_list
                write_test_info_to_csv(csv_writer,
                                       test_file_name,
                                       test_name,
                                       test_info,
                                       report_passing_tests)


def write_parsed_log_to_csv(logs_md,
                            parsed_log_dict,
                            csv_full_file_name,
                            report_passing_tests):
    with open(csv_full_file_name, "w") as csv_file:
        csv_writer = csv.writer(csv_file)
        write_parsed_log_csv_header(csv_writer)
        write_parsed_log_tests_to_csv(csv_writer,
                                      parsed_log_dict,
                                      report_passing_tests)

        csv_writer.writerow("")
        csv_writer.writerow([f"DESCRIPTION:{logs_md.description}"])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("unified_log_full_file_name",
                        metavar="[log file name]",
                        help="log file name generated by  "
                             "make_check_collect_logs")
    add_cmdline_arg_csv(parser)
    add_cmdline_optional_arg_report_pass(parser)
    mcu.cmdline_args = parser.parse_args()

    try:
        logs_md, parsed_logs = parse_unified_log(
            mcu.cmdline_args.unified_log_full_file_name)
        write_parsed_log_to_csv(logs_md, parsed_logs,
                                f"{mcu.cmdline_args.diff_csv_full_name}",
                                mcu.cmdline_args.report_pass)
    except FileNotFoundError:
        print(f"Error: {mcu.cmdline_args.unified_log_full_file_name} not "
              f"found")
        exit(1)

    print(f"CSV File written to {mcu.cmdline_args.diff_csv_full_name}")

    exit(get_exit_code())
