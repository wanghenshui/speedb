import sys
import make_check_collect_logs
import make_check_compare


if len(sys.argv) < 4:
    print(f"Usage: {sys.argv[0]} <reference "
          f"log file> <logs folder> <csv file name>")
    exit(1)

ref_log = sys.argv[1]
logs_folder = sys.argv[2]
csv_file_name = sys.argv[3]

new_log = "make_check_new.log"
print(f"Collecting Logs from {logs_folder} into {new_log}")
make_check_collect_logs.collect_logs_and_write_unified(logs_folder, new_log)

print(f"Comparing to {ref_log} reference file")
exit_code = \
    make_check_compare.compare_runs_and_write_diff_to_csv(ref_log,
                                                          new_log,
                                                          csv_file_name)
exit(exit_code)
