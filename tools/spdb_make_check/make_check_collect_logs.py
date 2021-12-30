import glob
import os
import sys
import re
import datetime
from make_check_utils import LogsMd, TestLogInfo, TestLogNameInfo, \
    UnifiedConsts, exit_code, report_exception


# Parses & Extract the log file name
def get_test_name_components(log_full_file_name):
    with open(log_full_file_name, "r", encoding="ISO-8859-1") as log_file:
        for log_line in log_file.readlines():
            test_start_line = re.findall(r"Note: Google Test filter =(.*)",
                                         log_line)
            if test_start_line:
                log_file_name = os.path.split(log_full_file_name)[1]
                test_name = test_start_line[0].strip()
                test_name_in_file_name = test_name.replace("/", "-")
                test_file_name_match = re.findall(fr".*log-run-(.*)-"
                                                  fr"{test_name_in_file_name}",
                                                  log_full_file_name)
                assert test_name, \
                    f"Empty test name (log_file_name:{log_file_name}\n" \
                    f"{log_line}"

                assert len(test_file_name_match) == 1,\
                    f"Failed matching single file" \
                    f" name  - {log_file_name}"

                return TestLogNameInfo(log_file_name=log_file_name,
                                       test_file_name=test_file_name_match[0],
                                       test_name=test_name)


def extract_test_file_name_from_log_file_name(log_file_name):
    test_file_name = log_file_name
    file_name_match = re.findall(r".*log-(.*)", log_file_name)
    if file_name_match:
        test_file_name = file_name_match[0].strip()
    return test_file_name


def get_num_test_cases_in_log(log_file_contents):
    num_cases_match = \
        re.findall(r"\[==========\] Running ([0-9]+) tests? from ",
                   log_file_contents, re.MULTILINE)
    if num_cases_match:
        return int(num_cases_match[0])
    else:
        return 0


def build_test_log_info(log_file_name,
                        test_file_name,
                        test_name,
                        test_log_lines):
    test_file_name_info = TestLogNameInfo(log_file_name=log_file_name,
                                          test_file_name=test_file_name,
                                          test_name=test_name)
    return TestLogInfo(name_info=test_file_name_info,
                       log_file_contents=test_log_lines)


def parse_multi_test_log_to_individual_logs(log_full_file_name,
                                            log_file_contents,
                                            all_logs_info):
    log_file_name = os.path.split(log_full_file_name)[1]
    log_lines = log_file_contents.splitlines()
    test_log_lines = ""

    test_file_name = \
        extract_test_file_name_from_log_file_name(log_full_file_name)
    test_name = ""

    for log_line in log_lines:
        test_start_line_match = re.findall(r"\[ RUN      \] (.*)", log_line)
        if test_start_line_match:
            assert not test_name, \
                f"Test start without detecting test end " \
                f"({log_full_file_name}), test name:{test_name}"
            test_name = test_start_line_match[0].strip()
            test_log_lines += log_line + "\n"
        else:
            test_end_line_match1 = re.findall(r"\[.*OK.*\] ", log_line)
            test_end_line_match2 = re.findall(r"\[.*FAILED.*\] ", log_line)

            if test_name and test_end_line_match1 or test_end_line_match2:
                test_log_lines += log_line + "\n"
                all_logs_info.append([build_test_log_info(log_file_name,
                                                          test_file_name,
                                                          test_name,
                                                          test_log_lines)])

                # reset test info
                test_name = ""
                test_log_lines = ""
            elif test_name:
                test_log_lines += log_line + "\n"

    if test_name:
        all_logs_info.append([build_test_log_info(log_file_name,
                                                  test_file_name,
                                                  test_name,
                                                  test_log_lines)])

    return all_logs_info


def collect_single_log_file(log_full_file_name, all_logs_info):
    with open(log_full_file_name, "r", encoding="ISO-8859-1") as log_file:
        log_file_name = os.path.split(log_full_file_name)[1]
        log_file_contents = log_file.read()

        num_tests = get_num_test_cases_in_log(log_file_contents)
        if num_tests == 1:
            test_file_name_info = get_test_name_components(log_full_file_name)
            if test_file_name_info:
                all_logs_info.append([
                    build_test_log_info(
                        log_file_name=test_file_name_info.log_file_name,
                        test_file_name=test_file_name_info.test_file_name,
                        test_name=test_file_name_info.test_name,
                        test_log_lines=log_file_contents)])
            else:
                all_logs_info.append([
                    build_test_log_info(log_file_name=log_full_file_name,
                                        test_file_name=log_full_file_name,
                                        test_name="UNKNOWN",
                                        log_file_contents=log_file_contents)])

        # In case there is more than 1 test in a single file
        elif num_tests > 1:
            parse_multi_test_log_to_individual_logs(log_full_file_name,
                                                    log_file_contents,
                                                    all_logs_info)

        else:
            test_file_name_info = get_test_name_components(log_full_file_name)
            if test_file_name_info:
                # Probably an empty log file (0 tests)
                all_logs_info.append([build_test_log_info(
                    log_file_name=test_file_name_info.log_file_name,
                    test_file_name=test_file_name_info.test_file_name,
                    test_name=test_file_name_info.test_name,
                    test_log_lines=log_file_contents)])
            else:
                # A log file with non-standard contents (probably just text
                # without the usual formatting)
                test_file_name = \
                    extract_test_file_name_from_log_file_name(
                        log_full_file_name)
                all_logs_info.append([build_test_log_info(
                    log_file_name=log_file_name,
                    test_file_name=test_file_name,
                    test_name="UNKNOWN",
                    test_log_lines=log_file_contents)])


def write_unified_logs_md(out_file, logs_folder, description):
    # Write Unified Logs Metadata
    out_file.write(f"{UnifiedConsts.MD_DELIM_LINE}\n")
    out_file.write(f"{LogsMd.DATE_FIELD_TEXT}{datetime.datetime.now()}\n")
    out_file.write(f"{LogsMd.FOLDER_FIELD_TEXT}{logs_folder}\n")
    out_file.write(f"{LogsMd.DESCRIPTION_FIELD_TEXT}{description}\n")
    out_file.write(f"{UnifiedConsts.MD_DELIM_LINE}\n")


def write_test_header(out_file, log_info):
    # Separator between the individual logs
    out_file.write(f"\n{UnifiedConsts.TEST_DELIM_LINE}\n")
    out_file.write(f"{UnifiedConsts.LOG_FILE_NAME_TITLE} "
                   f"{log_info[0].name_info.log_file_name}\n")
    out_file.write(f"{UnifiedConsts.FILE_TITLE} "
                   f"{log_info[0].name_info.test_file_name}\n")
    test_name = log_info[0].name_info.test_name
    is_disabled = test_name.find("DISABLED_") != -1
    test_name = test_name.replace("DISABLED_", "")
    out_file.write(f"{UnifiedConsts.TEST_TITLE} {test_name}\n")
    out_file.write(f"{UnifiedConsts.DISABLED_TITLE} ")

    if is_disabled:
        out_file.write("YES")
    else:
        out_file.write("NO")
    out_file.write("\n")

    out_file.write("\n")


def write_collected_logs_to_unified_file(all_logs_info, out_file):
    for log_info in all_logs_info:
        write_test_header(out_file, log_info)
        out_file.write(str(log_info[0].log_file_contents))

    # End of file / tests separator
    out_file.write(f"\n{UnifiedConsts.TEST_DELIM_LINE}\n")


#
# Collects individual test log files (created by make check) into a single file
# Returns a list of TestLogInfo objects, one per individual collected log
#
def collect_logs(logs_folder):
    # Generates a list of all log files in the logs_folder
    logs_file_names = glob.glob(f"{logs_folder}/log-*")

    # The list of info elements describing the individual tests
    # (one per test / test log file)
    all_logs_info = list()

    # Parsing each individual log file and populating the all_logs_info list
    for log_full_file_name in logs_file_names:
        try:
            collect_single_log_file(log_full_file_name, all_logs_info)
        except AssertionError as e:
            report_exception(f"Error processing "
                             f"{log_full_file_name}\n"
                             f"({e}) - ",
                             set_script_exit_code=True)

    all_logs_info.sort()
    return all_logs_info


def write_unified_logs_file(logs_folder,
                            all_logs_info,
                            unified_log_full_file_name,
                            description):
    # Build & Write the unified log file
    with open(unified_log_full_file_name, 'w') as out_file:
        write_unified_logs_md(out_file, logs_folder, description)
        write_collected_logs_to_unified_file(all_logs_info, out_file)


def collect_logs_and_write_unified(logs_folder, unified_log_full_file_name):
    all_logs_info = collect_logs(logs_folder)
    write_unified_logs_file(logs_folder,
                            all_logs_info,
                            unified_log_full_file_name)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <logs folder name> <output file name> ["
              f"<Description>]")
        exit(1)

    logs_folder = sys.argv[1]
    unified_log_full_file_name = sys.argv[2]

    description = ""
    if len(sys.argv) > 3:
        description = sys.argv[3]

    all_logs_info = collect_logs(logs_folder)
    write_unified_logs_file(logs_folder,
                            all_logs_info,
                            unified_log_full_file_name,
                            description)

    print(f"Logs collected to {unified_log_full_file_name}")

    exit(exit_code)
