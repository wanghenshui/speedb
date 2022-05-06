import csv
import argparse

import make_check_parse_log
import make_check_utils as mcu
from make_check_utils import NO_ITER_VALUE, DiffInfo, MismatchType,\
    TestResult, are_test_logs_with_iter_equal, TestInfo, ErrorInfo, \
    build_exception_info, report_exception, get_exit_code, \
    report_processing_errors, percentage_diff_str, \
    add_cmdline_arg_new_log,\
    add_cmdline_arg_ref_log,\
    add_cmdline_arg_csv,\
    add_cmdline_optional_arg_ignore_too_long,\
    add_cmdline_optional_arg_min_time,\
    add_cmdline_optional_arg_min_ref_time_increase, \
    add_cmdline_optional_arg_report_full_diff


UNKNOWN_TEST_NAME = ""

report_exceptions = False
# There may be diffs that will appear in the csv, but the runs will
# still be considered identical.
runs_are_identical = True


# Receives the information about the iterations of a single test
# The information is a list of ItersGroup instances. Each ItersGroup
# instance is a dictionary of the iterations in that group. The dictionary
# maps an iteration number to its TestInfo
# The function returns a dictionary of {iter_num: group_idx}
# For example, if a test has 3 iterations, belonging 2 two groups: [0, 2], [1]
# => iterations 0 and 2 belong to group 0, iteration 1 belongs to group 1
# The result will be: {"0": 0, "1": 1, "0": 0}
#
def get_iters_groupings(test_iter_groups):
    iters_to_grops_dict = dict()

    for group_idx, iters_group in enumerate(test_iter_groups):
        group_iters_to_group_dict = {iter_num_str: group_idx
                                     for iter_num_str in
                                     iters_group.get_iters_num_list()}
        iters_to_grops_dict_len_before_update = len(iters_to_grops_dict)
        iters_to_grops_dict.update(group_iters_to_group_dict)
        assert len(iters_to_grops_dict) == \
               iters_to_grops_dict_len_before_update + \
               len(group_iters_to_group_dict), "TBD - ADD An ERROR MESSAGE"

    return iters_to_grops_dict


# This function receives the result of running get_iters_groupings on
# the iterations of the same test in a ref and new runs => Two
# dictionaries, each one as described above The function pairs iterations in
# both runs and returns a mapping (Python dictionary) of identical groups to
# a list of iterations belonging to these groups. Iterations belonging to
# the same group may be treated identically
# in the diff process => They may be displayed as identical to the user.
# For example, for the input:
# ref_iters_grouping: {"0": 0, "1": 1, "2": 0}
# new_iters_grouping: {"0": 0, "1": 1, "2": 0}
#
# The output would be: {(0, 0): ["0", "2"], (1, 1): ["1"]}
#
# This output indicates that iterations 0 and 2 (in both the ref and new runs)
# are grouped => Both iterations 0 or 2 in the ref run have the same diff
# when compared with iterations 0 or 2 in the new run.
#
# Assumptions:
# len(ref_iters_grouping) == len(new_iters_grouping)
# For every element i in the lists, one of the following must be true:
# Element i is an integral number in both lists (its group number in
# that run and test); OR
# Element i in None (the same iteration is missing in both ref and new
# runs of this test [TBD - Is this acceptable / expected?]
#
def group_logically_equivalent_ref_and_new_iters(ref_iters_to_grops_dict,
                                                 new_iters_to_grops_dict):
    if set(ref_iters_to_grops_dict.keys()) != \
            set(new_iters_to_grops_dict.keys()):
        print("Heer")

    assert_msg = f"Mismatching ref and new iters groups: ref:" \
        f"{ref_iters_to_grops_dict}, new:{new_iters_to_grops_dict}"
    assert set(ref_iters_to_grops_dict.keys()) == \
           set(new_iters_to_grops_dict.keys()), assert_msg

    result = dict()
    for iter_num_str in set(ref_iters_to_grops_dict.keys()):
        ref_group_idx = ref_iters_to_grops_dict[iter_num_str]
        new_group_idx = new_iters_to_grops_dict[iter_num_str]

        pair = (ref_group_idx, new_group_idx)
        if pair not in result.keys():
            result[tuple(pair)] = list()
        result[tuple(pair)].append(iter_num_str)

    return result


def normalize_iters_for_diff_info(iterations):
    if iterations is None or iterations == NO_ITER_VALUE:
        return ""

    if type(iterations) is set:
        iterations = list(iterations)

    return [str(iter_num) for iter_num in iterations]


def build_diff_info(test_file_name,
                    test_name,
                    mismatch_type,
                    **kwargs):

    ref_result = kwargs["ref_result"] if ("ref_result" in kwargs) else ""
    new_result = kwargs["new_result"] if ("new_result" in kwargs) else ""
    ref_test_log_file_name = kwargs["ref_test_log_file_name"] if \
        ("ref_test_log_file_name" in kwargs) else ""
    ref_unified_log_line_num = kwargs["ref_unified_log_line_num"] if \
        ("ref_unified_log_line_num" in kwargs) else ""
    new_test_log_file_name = kwargs["new_test_log_file_name"] if \
        ("new_test_log_file_name" in kwargs) else ""
    new_unified_log_line_num = kwargs["new_unified_log_line_num"] if \
        ("new_unified_log_line_num" in kwargs) else ""
    iterations = normalize_iters_for_diff_info(kwargs["iterations"]) if \
        ("iterations" in kwargs) else ""
    ref_iter_params = kwargs["ref_iter_params"] if \
        ("ref_iter_params" in kwargs) else ""
    new_iter_params = kwargs["new_iter_params"] if \
        ("new_iter_params" in kwargs) else ""
    misc1 = kwargs["misc1"] if ("misc1" in kwargs) else ""
    misc2 = kwargs["misc2"] if ("misc2" in kwargs) else ""
    misc3 = kwargs["misc3"] if ("misc3" in kwargs) else ""

    return DiffInfo(test_file_name=test_file_name,
                    test_name=test_name,
                    mismatch_type=mismatch_type,
                    ref_result=ref_result,
                    new_result=new_result,
                    ref_test_log_file_name=ref_test_log_file_name,
                    ref_unified_log_line_num=ref_unified_log_line_num,
                    new_test_log_file_name=new_test_log_file_name,
                    new_unified_log_line_num=new_unified_log_line_num,
                    iterations=iterations,
                    ref_iter_params=ref_iter_params,
                    new_iter_params=new_iter_params,
                    misc1=misc1,
                    misc2=misc2,
                    misc3=misc3)


def append_diff(diff_to_append,
                diff_info,
                diff_makes_runs_different=True):
    if diff_makes_runs_different:
        global runs_are_identical
        runs_are_identical = False

    diff_info.append(diff_to_append)


def append_diff_file_missing_in_ref(new_test_file_name, diff_info):
    diff_to_append = build_diff_info(new_test_file_name,
                                     UNKNOWN_TEST_NAME,
                                     MismatchType.FILE_MISSING_IN_REF)
    append_diff(diff_to_append, diff_info)


def append_diff_file_missing_in_new(ref_test_file_name, diff_info):
    diff_to_append = build_diff_info(ref_test_file_name,
                                     UNKNOWN_TEST_NAME,
                                     MismatchType.FILE_MISSING_IN_NEW)
    append_diff(diff_to_append, diff_info)


def append_diff_test_missing_in_new(file_name, test_name, diff_info):
    diff_to_append = build_diff_info(file_name,
                                     test_name,
                                     MismatchType.TEST_MISSING_IN_NEW)
    append_diff(diff_to_append, diff_info)


def append_diff_test_missing_in_ref(file_name, test_name, new_test_info,
                                    diff_info, iterations=None):
    new_info = new_test_info
    diff_to_append =\
        build_diff_info(
            file_name,
            test_name,
            MismatchType.TEST_MISSING_IN_REF,
            new_result=new_info.result,
            new_test_log_file_name=new_info.header.name_info.log_file_name,
            new_unified_log_line_num=new_info.line_in_unified_log,
            iterations=iterations)
    append_diff(diff_to_append, diff_info)


def append_diff_test_iters_missing_in_new(file_name,
                                          test_name,
                                          ref_iters_not_in_new,
                                          ref_iters_groups_list,
                                          new_iters_groups_list,
                                          diff_info):
    # aux vars just to shorten the lines below
    first_ref_iter_test_info = ref_iters_groups_list[0].get_first_test()
    first_new_iter_test_info = new_iters_groups_list[0].get_first_test()
    ref_info = first_ref_iter_test_info
    new_info = first_new_iter_test_info

    diff_to_append =\
        build_diff_info(
            file_name,
            test_name,
            MismatchType.ITERS_MISSING_IN_NEW,
            ref_test_log_file_name=ref_info.header.name_info.log_file_name,
            ref_unified_log_line_num=ref_info.line_in_unified_log,
            new_test_log_file_name=new_info.header.name_info.log_file_name,
            new_unified_log_line_num=new_info.line_in_unified_log,
            iterations=ref_iters_not_in_new)
    append_diff(diff_to_append, diff_info)


def append_diff_test_iters_missing_in_ref(file_name,
                                          test_name,
                                          new_iters_not_in_ref,
                                          new_iters_groups_list,
                                          ref_iters_groups_list,
                                          diff_info):
    ref_info = ref_iters_groups_list[0].get_first_test()
    new_info = new_iters_groups_list[0].get_first_test()

    diff_to_append =\
        build_diff_info(
            file_name,
            test_name,
            MismatchType.ITERS_MISSING_IN_REF,
            ref_test_log_file_name=ref_info.header.name_info.log_file_name,
            ref_unified_log_line_num=ref_info.line_in_unified_log,
            new_test_log_file_name=new_info.header.name_info.log_file_name,
            new_unified_log_line_num=new_info.line_in_unified_log,
            iterations=new_iters_not_in_ref)
    append_diff(diff_to_append, diff_info)


def append_diff_full_details(file_name, test_name, mismatch_type,
                             ref_test_info, new_test_info,
                             diff_info,
                             diff_makes_runs_different=True,
                             **kwargs):
    ref_info = ref_test_info
    new_info = new_test_info
    diff_to_append =\
        build_diff_info(
            file_name,
            test_name,
            mismatch_type,
            ref_result=ref_info.result,
            ref_test_log_file_name=ref_info.header.name_info.log_file_name,
            ref_unified_log_line_num=ref_info.line_in_unified_log,
            new_result=new_info.result,
            new_test_log_file_name=new_info.header.name_info.log_file_name,
            new_unified_log_line_num=new_info.line_in_unified_log,
            **kwargs)
    append_diff(diff_to_append, diff_info, diff_makes_runs_different)


def append_diff_mismatching_results(file_name, test_name, ref_test_info,
                                    new_test_info, iterations, diff_info):
    append_diff_full_details(file_name,
                             test_name,
                             MismatchType.MISMATCHING_RESULTS,
                             ref_test_info,
                             new_test_info,
                             diff_info,
                             iterations=iterations)


def append_diff_mismatching_logs(file_name, test_name, ref_test_info,
                                 new_test_info, iterations, diff_info,
                                 diff_lines=""):
    if mcu.cmdline_args.report_full_diff:
        misc1 = diff_lines
        misc2 = ref_test_info.log
        misc3 = new_test_info.log
    else:
        misc1 = "Check Logs of both tests to find the differences"
        misc2 = None
        misc3 = None

    append_diff_full_details(file_name,
                             test_name,
                             MismatchType.MISMATCHING_LOGS,
                             ref_test_info,
                             new_test_info,
                             diff_info,
                             iterations=iterations,
                             misc1=misc1,
                             misc2=misc2,
                             misc3=misc3)


tool_long_count = 0


def append_diff_new_test_too_long(file_name, test_name, ref_test_info,
                                  new_test_info, diff_info):
    global tool_long_count
    tool_long_count += 1

    ref_time_increase_str = percentage_diff_str(ref_test_info.time_ms,
                                                new_test_info.time_ms)

    append_diff_full_details(file_name,
                             test_name,
                             MismatchType.NEW_TEST_TOOK_TOO_LONG,
                             ref_test_info,
                             new_test_info,
                             diff_info,
                             diff_makes_runs_different=False,
                             misc1=f"ref time (ms) = {ref_test_info.time_ms}",
                             misc2=f"new time (ms) = {new_test_info.time_ms}"
                                   f" ({ref_time_increase_str})")


def append_diff_mismatching_num_of_iters(file_name,
                                         test_name,
                                         ref_test_iters_infos_list,
                                         new_test_iters_infos_list,
                                         diff_info):
    # Where applicable, use the first list element for details
    # (iterations start from 0 onwards)
    ref_info = ref_test_iters_infos_list[0]
    new_info = new_test_iters_infos_list
    diff_to_append =\
        build_diff_info(file_name,
                        test_name,
                        MismatchType.MISMATCHING_NUM_OF_ITERS,
                        ref_unified_log_line_num=ref_info.line_in_unified_log,
                        new_unified_log_line_num=new_info.line_in_unified_log,
                        misc1=f"Ref Test Has "
                              f"{len(ref_test_iters_infos_list)} Iterations",
                        misc2=f"New Test Has "
                              f"{len(new_test_iters_infos_list)} Iterations")
    append_diff(diff_to_append, diff_info)


def append_diff_mismatching_iter_params(file_name,
                                        test_name,
                                        ref_test_info,
                                        new_test_info,
                                        diff_info):
    append_diff_full_details(file_name,
                             test_name,
                             MismatchType.MISMATCHING_ITER_PARAMETERS,
                             ref_test_info,
                             new_test_info,
                             diff_info,
                             iterations=ref_test_info.header.iter_num,
                             misc1=f"Ref-Iter-Params ="
                                   f" {ref_test_info.iter_params}",
                             misc2=f"New-Iter-Params ="
                                   f" {new_test_info.iter_params}")


def append_diff_test_with_iters_in_ref_no_iters_in_new(file_name,
                                                       test_name,
                                                       diff_info):
    diff_to_append =\
        build_diff_info(file_name,
                        test_name,
                        MismatchType.TEST_WITH_ITERS_IN_REF_NO_ITERS_IN_NEW)
    append_diff(diff_to_append, diff_info)


def append_diff_new_test_took_too_long_if_applicable(file_name,
                                                     test_name,
                                                     ref_test_info,
                                                     new_test_info,
                                                     diff_info):
    if mcu.cmdline_args.ignore_too_long:
        return

    # Report successful tests (ref & new) whose new run
    # time increased by more than 30%
    ref_test_time = ref_test_info.time_ms
    new_test_time = new_test_info.time_ms

    if ((ref_test_time >= mcu.cmdline_args.min_ref_time or
         new_test_time >= mcu.cmdline_args.min_ref_time)
            and
            (new_test_time >=
             int((ref_test_time *
                  (100 + mcu.cmdline_args.ref_time_increase)) / 100))):
        append_diff_new_test_too_long(file_name,
                                      test_name,
                                      ref_test_info,
                                      new_test_info,
                                      diff_info)


def compare_tests(file_name,
                  test_name,
                  ref_test_info,
                  new_test_info,
                  iterations,
                  diff_info):
    ref_test_result = ref_test_info.result
    new_test_result = new_test_info.result

    if ref_test_result != new_test_result:
        append_diff_mismatching_results(file_name,
                                        test_name,
                                        ref_test_info,
                                        new_test_info,
                                        iterations,
                                        diff_info)

    elif ref_test_result == TestResult.PASS:
        append_diff_new_test_took_too_long_if_applicable(file_name,
                                                         test_name,
                                                         ref_test_info,
                                                         new_test_info,
                                                         diff_info)
    else:
        # Report tests that FAILED identically but their logs indicate a
        # mismatch that needs further investigation
        are_logs_equal, diff_lines =\
            are_test_logs_with_iter_equal(test_name,
                                          ref_test_info,
                                          new_test_info)

        if not are_logs_equal:
            append_diff_mismatching_logs(file_name,
                                         test_name,
                                         ref_test_info,
                                         new_test_info,
                                         iterations,
                                         diff_info,
                                         diff_lines)


def expand_test_iters_info(test_iters_infos_list):
    expanded_info = dict()

    for test_iters_info in test_iters_infos_list:
        iters = test_iters_info["iters"]
        for iter_num in iters:
            test_iters_info_copy = test_iters_info.copy()
            del test_iters_info_copy["iters"]
            expanded_info[iter_num] = test_iters_info_copy

    return expanded_info


# Returns a list of TestInfo, one per iteration of a test with iterations
def get_tests_iters_list(iters_group_list):
    non_sorted_iters_list = [(test_info.header.iter_num, test_info)
                             for iters_group in iters_group_list
                             for test_info in iters_group.get_tests_list()]

    return sorted(non_sorted_iters_list)


# Verifies that a test with iterations is considered eligible for grouping and
# comparison of its individual iterations
# The test must meet all of the following conditions:
# 1. Have the exact same iterations in both runs (based on the
# iteration number)
# 2. Matching iterations have the exact same parameters (if available
# in the log)
#
# Failure to meet one of these conditions means that the tests are not the same
# in both runs in the semantic level and therefore may not be compared at the
# iteration level.
def check_if_test_iters_can_be_grouped_and_report_diff(file_name,
                                                       test_name,
                                                       ref_iters_groups_list,
                                                       ref_iters_to_grops_dict,
                                                       new_iters_groups_list,
                                                       new_iters_to_grops_dict,
                                                       diff_info):
    answer = True

    # First verify that both runs of the test have the exact same iterations
    ref_iters_nums_strs = set(ref_iters_to_grops_dict.keys())
    new_iters_nums_strs = set(new_iters_to_grops_dict.keys())

    ref_iters_not_in_new = ref_iters_nums_strs - new_iters_nums_strs
    new_iters_not_in_ref = new_iters_nums_strs - ref_iters_nums_strs

    if ref_iters_not_in_new or new_iters_not_in_ref:
        if ref_iters_not_in_new:
            append_diff_test_iters_missing_in_new(file_name,
                                                  test_name,
                                                  list(ref_iters_not_in_new),
                                                  ref_iters_groups_list,
                                                  new_iters_groups_list,
                                                  diff_info)
        if new_iters_not_in_ref:
            append_diff_test_iters_missing_in_ref(file_name,
                                                  test_name,
                                                  list(new_iters_not_in_ref),
                                                  new_iters_groups_list,
                                                  ref_iters_groups_list,
                                                  diff_info)
        return False

    # Now verify that matching iterations have the same parameters
    ref_tests_iters_list = get_tests_iters_list(ref_iters_groups_list)
    new_tests_iters_list = get_tests_iters_list(new_iters_groups_list)
    assert len(ref_tests_iters_list) == len(new_tests_iters_list)

    for i in range(len(ref_tests_iters_list)):
        ref_test_info = ref_tests_iters_list[i][1]
        new_test_info = new_tests_iters_list[i][1]
        if ref_test_info.header.iter_num != new_test_info.header.iter_num:
            print("BUG")
        assert ref_test_info.header.iter_num == new_test_info.header.iter_num

        if ref_test_info.iter_params and new_test_info.iter_params and \
                ref_test_info.iter_params != new_test_info.iter_params:
            answer = False
            append_diff_mismatching_iter_params(file_name,
                                                test_name,
                                                ref_test_info,
                                                new_test_info,
                                                diff_info)

    return answer


# The function compares a single test with iterations in the ref and new runs.
#
# A parametrized Google test has as many iterations as the combination of its
# parameters which were used when running it. These iterations are
# numbered from 0 to (Num iterations - 1).
# The tests' parameters themselves are logged only on failures => unknown
# to the script otherwise.
# The script assumes that iteration X in both runs is the same
# (tests the same aspects).
# The ref_iters_groups_list and new_iters_groups_list are lists of
# groups of iterations that are logically-equivalent in their run.
# The function attempts to find groups of iterations in both runs,
# that are logically equivalent.
# That means that their result is the same and their associated logs
# are deemed identical.
# However, only tests that have the exact same iterations in both runs are
# subject to this classification
#
def compare_tests_with_iters(file_name, test_name, ref_iters_groups_list,
                             new_iters_groups_list, diff_info):
    assert type(ref_iters_groups_list) is list, \
        f"Unexpected ref_test_info type ({type(ref_iters_groups_list)}. " \
        f"Expected list. "
    assert type(new_iters_groups_list) is list, \
        f"Unexpected ref_test_info type ({type(new_iters_groups_list)}. " \
        f"Expected list. "

    ref_iters_to_groups_dict = get_iters_groupings(ref_iters_groups_list)
    new_iters_to_groups_dict = get_iters_groupings(new_iters_groups_list)

    if not check_if_test_iters_can_be_grouped_and_report_diff(
            file_name,
            test_name,
            ref_iters_groups_list,
            ref_iters_to_groups_dict,
            new_iters_groups_list,
            new_iters_to_groups_dict,
            diff_info):
        return

    # test has the exact same iterations in both runs => classify to
    # find identical groups
    # The output of the classification is a dictionary mapping the tuple
    # (ref group index, new group index) to
    # a list of iterations in both runs.
    # For example: {(0, 0): ["0", "2"], (1, 1): ["1"]}
    groups_pair_to_iters_list_dict = \
        group_logically_equivalent_ref_and_new_iters(ref_iters_to_groups_dict,
                                                     new_iters_to_groups_dict)

    # Now, per resulting group, compare the first iter of the ref group
    # with the first of the new group (all iters in the ref run in the same
    # group are equivalent to all iters in the corresponding group in the
    # new run)

    ref_tests_iters_list = get_tests_iters_list(ref_iters_groups_list)
    new_tests_iters_list = get_tests_iters_list(new_iters_groups_list)

    for groups_pair, iterations in groups_pair_to_iters_list_dict.items():
        first_ref_iter_test_info = ref_tests_iters_list[iterations[0]][1]
        first_new_iter_test_info = new_tests_iters_list[iterations[0]][1]

        compare_tests(file_name,
                      test_name,
                      first_ref_iter_test_info,
                      first_new_iter_test_info,
                      iterations,
                      diff_info)


def compare_ref_test_no_iters_with_new_test(ref_test_file_name,
                                            ref_test_name,
                                            ref_test_info,
                                            new_test_info,
                                            diff_info):
    assert ref_test_info.header.iter_num == NO_ITER_VALUE,\
        f"ref test should have no iterations " \
        f"({ref_test_file_name} - {ref_test_name} - {ref_test_info}"

    if type(new_test_info) is list:
        append_diff(
            build_diff_info(
                ref_test_file_name,
                ref_test_name,
                MismatchType.TEST_NO_ITERS_IN_REF_WITH_ITERS_IN_NEW))
    else:
        # the test has no iterations in both ref and new =>
        # compare them in their details
        assert type(new_test_info) is TestInfo, \
            f"new_test_info has Unexpected Type " \
            f"({type(new_test_info)}). Expected TestInfo"

        compare_tests(ref_test_file_name,
                      ref_test_name,
                      ref_test_info,
                      new_test_info,
                      None,
                      diff_info)


def compare_ref_test_with_iters_with_new_test(
        file_name,
        test_name,
        iters_groups_list,
        new_test_info_or_iters_groups_list,
        diff_info):
    assert type(iters_groups_list) is list, \
        f"Unexpected ref_test_info type ({type(iters_groups_list)}. " \
        f"Expected list. "

    if type(new_test_info_or_iters_groups_list) is TestInfo:
        append_diff_test_with_iters_in_ref_no_iters_in_new(file_name,
                                                           test_name,
                                                           diff_info)

    else:
        assert type(new_test_info_or_iters_groups_list) is list, \
            f"Unexpected ref_test_info type " \
            f"({type(new_test_info_or_iters_groups_list)}. Expected list. "

        # the test has iterations in both ref and new => compare
        # them in their details
        compare_tests_with_iters(file_name,
                                 test_name,
                                 iters_groups_list,
                                 new_test_info_or_iters_groups_list,
                                 diff_info)


def append_new_files_and_tests_missing_in_ref(ref_tests,
                                              new_tests,
                                              diff_info,
                                              processing_errors):
    for new_test_file_name, new_tests_in_file in new_tests.items():
        try:
            if new_test_file_name in ref_tests.keys():
                ref_tests_in_file = ref_tests[new_test_file_name]
                for new_test_name, new_test_info in new_tests_in_file.items():
                    if new_test_name not in ref_tests_in_file.keys():
                        if type(new_test_info) is list:
                            first_new_test_info = \
                                new_test_info[0].get_first_test()
                            iterations = [str(iter_num)
                                          for iters_group in new_test_info
                                          for iter_num in
                                          iters_group.get_iters_num_list()]
                        else:
                            first_new_test_info = new_test_info
                            iterations = None
                        append_diff_test_missing_in_ref(new_test_file_name,
                                                        new_test_name,
                                                        first_new_test_info,
                                                        diff_info,
                                                        iterations)
            else:
                append_diff_file_missing_in_ref(new_test_file_name, diff_info)

        except AssertionError as e:
            processing_errors.append(
                ErrorInfo(description="Error while appending new files",
                          test_file_name=new_test_file_name,
                          exc_info=build_exception_info(e)))


def compare_runs(ref_log_file_name, new_log_file_name):
    print(f"Comparing {ref_log_file_name} with {new_log_file_name}")

    processing_errors = list()
    diff_info = list()

    try:
        ref_logs_md, ref_tests = make_check_parse_log.parse_unified_log(
            ref_log_file_name)
        new_logs_md, new_tests = make_check_parse_log.parse_unified_log(
            new_log_file_name)
    except FileNotFoundError as e:
        print(e)
        exit(2)

    # The loop handles the following:
    # - Ref files that are missing in new run
    # - Ref tests that are missing in the same test file in the new run
    # - Comparing ref tests with new tests given they are in the same
    # file and have the same test name
    for ref_test_file_name, ref_tests_in_file in ref_tests.items():
        try:
            if ref_test_file_name in new_tests.keys():
                # Both runs have tests for this file
                new_tests_in_file = new_tests[ref_test_file_name]

                # Iterator over all tests in the ref test file
                for ref_test_name, ref_test_info in ref_tests_in_file.items():
                    try:
                        if ref_test_name not in new_tests_in_file.keys():
                            append_diff_test_missing_in_new(ref_test_file_name,
                                                            ref_test_name,
                                                            diff_info)
                        else:
                            new_test_info = new_tests_in_file[ref_test_name]

                            # ref_test_info is either a single TestInfo
                            # (test without iterations) or a list of
                            # ItersGroup objects, one per groups of
                            # logically-equivalent iterations.
                            # Each IterGroup is a dictionary of all test
                            # iterations in that group (new_test_info may
                            # or may not have iterations)
                            if type(ref_test_info) is TestInfo:
                                compare_ref_test_no_iters_with_new_test(
                                    ref_test_file_name,
                                    ref_test_name,
                                    ref_test_info,
                                    new_test_info,
                                    diff_info)
                            # ref_test_info is a list => ref test has
                            # iterations (new test may or may not)
                            else:
                                compare_ref_test_with_iters_with_new_test(
                                    ref_test_file_name,
                                    ref_test_name,
                                    ref_test_info,
                                    new_test_info,
                                    diff_info)
                    except AssertionError as e:
                        if (report_exceptions):
                            processing_errors.append(
                                ErrorInfo(description="Error while comparing",
                                          test_file_name=ref_test_file_name,
                                          test_name=ref_test_name,
                                          exc_info=build_exception_info(e)))
                        else:
                            raise
            else:
                append_diff_file_missing_in_new(ref_test_file_name, diff_info)
        except AssertionError as e:
            if (report_exceptions):
                processing_errors.append(
                    ErrorInfo(description="Error while comparing",
                              test_file_name=ref_test_file_name,
                              test_name=ref_test_name,
                              exc_info=build_exception_info(e)))
            else:
                raise

    # What's left is to handle files / tests that exist in the new run
    # but are missing in the ref run
    append_new_files_and_tests_missing_in_ref(ref_tests,
                                              new_tests,
                                              diff_info,
                                              processing_errors)

    diff_info.sort()

    for diff_line in diff_info:
        assert type(diff_line) is DiffInfo,\
            f"Unexpected diff_line type {type(diff_line)}. Expected DiffInfo"

    return ref_logs_md, new_logs_md, diff_info, processing_errors


def write_diff_info_to_csv(ref_logs_md, new_logs_md, diff_info,
                           csv_full_file_name):
    with open(csv_full_file_name, "w") as csv_file:
        csv_writer = csv.writer(csv_file)

        csv_writer.writerow(DiffInfo.get_fields_as_csv_header())
        for diff_line in diff_info:
            csv_writer.writerow(diff_line.get_as_csv_row())

        csv_writer.writerow("")
        csv_writer.writerow([f"REF: {ref_logs_md.description}"])
        csv_writer.writerow([f"NEW: {new_logs_md.description}"])

    print(f"Diff written to {csv_full_file_name}")


def compare_runs_and_write_diff_to_csv(ref_log_file_name,
                                       new_log_file_name,
                                       csv_full_file_name):
    try:
        ref_logs_md, new_logs_md, diff_info, processing_errors = \
            compare_runs(ref_log_file_name,
                         new_log_file_name)
        if processing_errors:
            report_processing_errors("Errors while comparing runs:",
                                     processing_errors,
                                     set_script_exit_code=True)

    except AssertionError as e:
        if (report_exceptions):
            report_exception(f"Exception while comparing {ref_log_file_name} "
                             f"and {new_log_file_name}\n"
                             f" ({e})\n",
                             set_script_exit_code=True)
        else:
            raise

    print(f"\nNew Run Logs are in {new_log_file_name}")

    if diff_info:
        write_diff_info_to_csv(ref_logs_md, new_logs_md, diff_info,
                               csv_full_file_name)

    exit_code = get_exit_code()
    if exit_code == 0 and not runs_are_identical:
        exit_code = 1

    if exit_code == 0:
        print("\nRuns Are Identical")
    else:
        print("\nRuns Are Different")

    return exit_code


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    add_cmdline_arg_ref_log(parser)
    add_cmdline_arg_new_log(parser)
    add_cmdline_arg_csv(parser)
    add_cmdline_optional_arg_ignore_too_long(parser)
    add_cmdline_optional_arg_min_time(parser)
    add_cmdline_optional_arg_min_ref_time_increase(parser)
    add_cmdline_optional_arg_report_full_diff(parser)
    mcu.cmdline_args = parser.parse_args()

    if not mcu.check_cmdline_conflicts():
        exit(2)

    exit_code = compare_runs_and_write_diff_to_csv(
        mcu.cmdline_args.ref_log_full_name,
        mcu.cmdline_args.new_log_full_name,
        mcu.cmdline_args.diff_csv_full_name)

    exit(exit_code)
