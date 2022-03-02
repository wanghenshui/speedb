#!/bin/sh
set -eo pipefail

bail() {
	echo 1>&2 "$1"
	exit 1
}

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
	bail "usage: $0 [<merge-branch>] <target-branch>"
fi

# Check for the existence of git
command -v git >/dev/null || exit 1

SCRIPT_ROOT=$(dirname "$0")

# Get the repo's root dir
# Use `cd` and a subshell instead of `git -C`, in order to support old git versions
REPO_ROOT=$(cd "$SCRIPT_ROOT" && git rev-parse --show-toplevel)

# Assume HEAD if no merge branch was provided
if [ $# -lt 2 ]; then
	MERGE_BRANCH=$(cd "$REPO_ROOT" && git rev-parse --abbrev-ref HEAD)
	TARGET_BRANCH=$1
else
	MERGE_BRANCH=$1
	TARGET_BRANCH=$2
fi

# Extract the JIRA key from the branch name
JIRA_KEY=$(echo "$MERGE_BRANCH" | (grep -o '[A-Z]\+-[0-9]\+' ||:) | head -1)
if [ -z "$JIRA_KEY" ]; then
	bail "error: branch name $MERGE_BRANCH does not contain a JIRA key"
fi

FETCH_MAX_DEPTH=${FETCH_MAX_DEPTH:-50}

# Make sure we have access to target and merge branches
REMOTE=$(cd "$REPO_ROOT" && git remote)
(cd "$REPO_ROOT" && git fetch -p --depth="$FETCH_MAX_DEPTH" "$REMOTE" "$TARGET_BRANCH")
(cd "$REPO_ROOT" && git fetch -p --depth="$FETCH_MAX_DEPTH" "$REMOTE" "$MERGE_BRANCH")

# Check that the branch being merged is indeed based on the target branch
if ! (cd "$REPO_ROOT" && git merge-base --is-ancestor "$REMOTE/$TARGET_BRANCH" "$REMOTE/$MERGE_BRANCH"); then
	bail "error: $TARGET_BRANCH is not an ancestor of $MERGE_BRANCH"
fi

# Verify that we pass code style checks for the changes that were made
if ! (cd "$REPO_ROOT" && FORMAT_UPSTREAM="$REMOTE/$TARGET_BRANCH" ./tools/spdb-check-diff.sh 1>&2); then
	bail "error: some files require formatting. Please check the output above"
fi

# Verify that all of the commits contain the right JIRA key
(cd "$REPO_ROOT" && git log --format="%h %s" "$REMOTE/$TARGET_BRANCH..$REMOTE/$MERGE_BRANCH") | while read -r line; do
	m=$(echo "$line" | sed 's/[0-9a-f]\+ //')
	if ! echo "$m" | grep -q "^$JIRA_KEY: "; then
		h=$(echo "$line" | sed 's/\([0-9a-f]\+\) .*$/\1/')
		bail "error: commit message for $h doesn't begin with JIRA key $JIRA_KEY"
	fi
done

# Print the JIRA key for consumption by other scripts
echo "$JIRA_KEY"