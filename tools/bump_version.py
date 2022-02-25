#!/usr/bin/env python3
import os
import re
import argparse
import operator
import subprocess


class BumpVersionError(Exception):
    pass


version_elements = (b"SPEEDB_MAJOR", b"SPEEDB_MINOR", b"SPEEDB_PATCH")


def parse_version(version_header_dump):
    """Parse the version from the given version header dump.

    Args:
        version_header_dump (bytes): a dump of the version header content.

    Returns:
        tuple. Contains the value of each version element that is found,
        in the same order as `version_elements`.

    Raises:
        BumpVersionError. Raised if one of the version elements isn't defined.
    """
    version = []
    for version_element in version_elements:
        version_element_value = re.search(
            rb"define\s+%b\s+([0-9]+)" % version_element, version_header_dump
        )
        if version_element_value is None:
            raise BumpVersionError(f"{version_element} isn't defined")
        version.append(int(version_element_value.group(1)))

    return tuple(version)


def update_version(target_branch, version_header):
    """Update the version in the given version header file.

    Args:
        target_branch (str): the branch we are trying to merge into.
        version_header (str): path to the version header file.

    Returns:
        bool. True if we updated the version successfully, False otherwise.
    """
    # try to get the version on the source branch (current branch)
    with open(version_header, "rb") as source_version_file:
        source_version = parse_version(source_version_file.read())

    # try to get the version on the target branch
    tree_content = subprocess.check_output(
        ["git", "ls-tree", target_branch, version_header]
    )
    if not tree_content:
        print(f"{version_header} doesn't exist on the target branch, doing nothing")
        return False

    # Only make 3 splits at most
    obj_size, obj_type, obj_hash, obj_name = tree_content.split(None, 3)
    assert obj_type == b"blob"  # Sanity check
    version_dump = subprocess.check_output(["git", "show", obj_hash])
    target_version = parse_version(version_dump)
    # try to update the version on the source branch
    # according to the existing one on the target
    expected_version = tuple(map(operator.add, target_version, (0, 0, 1)))
    if source_version == expected_version:
        print(
            "The version on the current branch is already the expected one, "
            "doing nothing"
        )
        return False

    if source_version[:2] > expected_version[:2]:
        print(
            "The version on the current branch is bigger than the one on "
            f"{target_branch}, doing nothing"
        )
        return False

    for version_element, version_element_value in zip(
        version_elements, expected_version
    ):
        version_dump = re.sub(
            rb"define\s+%b\s+[0-9]+" % version_element,
            b"define %b %d" % (version_element, version_element_value),
            version_dump,
        )

    with open(version_header, "wb") as version_header_file:
        version_header_file.write(version_dump)

    return True


def is_ancestor_ref(ref, ancestor):
    """Check if a git ref is based on another git ref.

    Args:
        ref (str): the git ref to check.
        ancestor (str): the git ref that's supposed to be an ancestor of `ref`.

    Returns:
        bool. True if `ancestor` is an ancestor of `ref`, False otherwise.
    """
    try:
        subprocess.check_call(["git", "merge-base", "--is-ancestor", ancestor, ref])
    except subprocess.CalledProcessError:
        return False

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", help="path to the version header file")
    parser.add_argument("--target", default="master", help="target branch")
    parser.add_argument(
        "-C", "--commit", action="store_true", help="amend and commit the changes"
    )
    args = parser.parse_args()
    version_header = args.version
    target_branch = args.target
    commit = args.commit

    remote = (
        subprocess.check_output(["git", "remote", "show"])
        .strip()
        .decode("utf-8")
        .split()
    )
    assert len(remote) > 0, "no git remotes found"
    # Prefer "origin" if exists
    if "origin" in remote:
        remote = "origin"
    else:
        remote = remote[0]

    # Make sure we have access to the target tree
    remote_target_branch = f"{remote}/{target_branch}"
    target = (
        subprocess.check_output(
            ["git", "branch", "--list", "--remotes", remote_target_branch]
        )
        .strip()
        .decode("utf-8")
    )
    if target != remote_target_branch or not is_ancestor_ref(
        "HEAD", remote_target_branch
    ):
        subprocess.check_call(["git", "fetch", "--depth=1", remote, target_branch])

    # For the version bump to make sense, we need to ensure that the current branch
    # is indeed based on the target branch
    assert is_ancestor_ref(
        "HEAD", remote_target_branch
    ), f"expected {target_branch} to be an ancestor of the current branch"

    if update_version(remote_target_branch, version_header) and commit:
        # Get the user name and Email for the last commit
        author_name = subprocess.check_output(
            ["git", "log", "-1", "--pretty=format:%an"]
        )
        author_email = subprocess.check_output(
            ["git", "log", "-1", "--pretty=format:%ae"]
        )
        git_committer_env = os.environ.copy()
        git_committer_env["GIT_AUTHOR_NAME"] = git_committer_env[
            "GIT_COMMITTER_NAME"
        ] = author_name
        git_committer_env["GIT_AUTHOR_EMAIL"] = git_committer_env[
            "GIT_COMMITTER_EMAIL"
        ] = author_email

        # Amend the change
        subprocess.check_call(["git", "add", version_header])
        subprocess.check_call(
            ["git", "commit", "--amend", "--no-edit"], env=git_committer_env
        )
