#!/bin/sh
print_usage () {
  echo "Usage:"
  echo "diff-check.sh [OPTIONS]"
  echo "-h: print this message."
}

while getopts ':h' OPTION; do
  case "$OPTION" in
    h)
      print_usage
      exit
      ;;
    ?)
      print_usage
      exit 1
      ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "$0")" && git rev-parse --show-toplevel)"

set -e

if [ "$LINT_DIFFS" ]; then
  echo "Note: LINT_DIFFS='$LINT_DIFFS'"
  # Dry run to confirm it works
  $LINT_DIFFS --help >/dev/null < /dev/null || exit 128
else
  # First try directly executing the possibilities
  if lint-diffs --help > /dev/null 2>&1 < /dev/null; then
    LINT_DIFFS="lint-diffs --strict"
  else
    echo "We couldn't find lint-diffs on your computer!"
    echo "You can install lint-diffs by running: "
    echo "    python -m pip install lint-diffs"
    echo "Then make sure clang-format is available and executable from \$PATH:"
    echo "    clang-format --version"
    exit 128
  fi
fi

# If there's no uncommitted changes, we assume user are doing post-commit
# format check, in which case we'll try to check the modified lines vs. the
# master branch. Otherwise, we'll check format of the uncommitted code only.
if [ -z "$(cd "$REPO_ROOT" && git diff HEAD)" ]; then
  # Attempt to get name of facebook/rocksdb.git remote.
  [ "$FORMAT_REMOTE" ] || FORMAT_REMOTE="$(cd "$REPO_ROOT" && git remote -v | grep 'speedb/speedb.git' | head -n 1 | cut -f 1)"
  # Fall back on 'origin' if that fails
  [ "$FORMAT_REMOTE" ] || FORMAT_REMOTE=origin
  # Use master branch from that remote
  [ "$FORMAT_UPSTREAM" ] || FORMAT_UPSTREAM="$FORMAT_REMOTE/$(cd "$REPO_ROOT" && LC_ALL=POSIX LANG=POSIX git remote show $FORMAT_REMOTE | sed -n '/HEAD branch/s/.*: //p')"
  # Get the common ancestor with that remote branch. Everything after that
  # common ancestor would be considered the contents of a merge request, so
  # should be relevant for formatting fixes.
  FORMAT_UPSTREAM_MERGE_BASE="$(cd "$REPO_ROOT" && git merge-base "$FORMAT_UPSTREAM" HEAD)"
  # Get the differences
  echo "Checking format of changes not yet in $FORMAT_UPSTREAM..."
  (cd "$REPO_ROOT" && git diff -U0 "$FORMAT_UPSTREAM_MERGE_BASE" | $LINT_DIFFS)
else
  # Check the format of uncommitted lines,
  echo "Checking format of uncommitted changes..."
  (cd "$REPO_ROOT" && git diff -U0 HEAD | $LINT_DIFFS)
fi
