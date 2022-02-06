#!/usr/bin/env python3
import re
import os
import argparse
import operator
import subprocess

class BumpVersionError(Exception):
    pass

version_elements = (b'SPEEDB_MAJOR', b'SPEEDB_MINOR', b'SPEEDB_PATCH')

def parse_version(version_header_dump):
    '''Parse the version from the given version header dump.

    Args:
        version_header_dump (bytes): a dump of the version header content.

    Returns:
        tuple. Contains the value of each version element that is found, in the same order
        as `version_elements`.

    Raises:
        BumpVersionError. Raised if one of the version elements isn't defined.
    '''
    version = []
    for version_element in version_elements:
        version_element_value = re.search(rb'define\s+%b\s+([0-9]+)' % version_element, version_header_dump)
        if version_element_value is None:
            raise BumpVersionError(f"{version_element} isn't defined")
        version.append(int(version_element_value.group(1)))

    return tuple(version)

def update_version(target_branch, version_header):
    '''Update the version in the given version header file.

    Args:
        target_branch (str): the branch we are trying to merge into.
        version_header (str): path to the version header file.
    '''
    # try to get the version on the source branch (current branch)
    with open(version_header, 'rb') as source_version_file:
        source_version = parse_version(source_version_file.read())

    # try to get the version on the target branch
    tree_content = subprocess.check_output(['git', 'ls-tree', target_branch, version_header])
    if not tree_content:
        print(f"{version_header} doesn't exist on the target branch, doing nothing")
    else:
        obj_size, obj_type, obj_hash, obj_name = tree_content.split(None, 3)  # Only make 3 splits at most
        assert obj_type == b'blob'  # Sanity check
        version_dump = subprocess.check_output(['git', 'show', obj_hash])
        target_version = parse_version(version_dump)
        # try to update the version on the source branch according to the existing one on the target
        expected_version = tuple(map(operator.add, target_version, (0, 0, 1)))
        if source_version[:2] > expected_version[:2] or source_version == expected_version:
            print(f'The version on the current branch is bigger than the one on {target_branch}, doing nothing')
        else:
            for version_element, version_element_value in zip(version_elements, expected_version):
                version_dump = re.sub(rb'define\s+%b\s+[0-9]+' % version_element,
                                      b'define %b %d' % (version_element, version_element_value),
                                      version_dump)

            with open(version_header, 'wb') as version_header_file:
                version_header_file.write(version_dump)

            # Get the user name and Email for the last commit
            author_name = subprocess.check_output(['git', 'log', '-1', '--pretty=format:%an'])
            author_email = subprocess.check_output(['git', 'log', '-1', '--pretty=format:%ae'])
            git_committer_env = os.environ.copy()
            git_committer_env['GIT_AUTHOR_NAME'] = git_committer_env['GIT_COMMITTER_NAME'] = author_name
            git_committer_env['GIT_AUTHOR_EMAIL'] = git_committer_env['GIT_COMMITTER_EMAIL'] = author_email

            # Amend the change
            subprocess.check_call(['git', 'add', version_header])
            subprocess.check_call(['git', 'commit', '--amend', '--no-edit'], env=git_committer_env)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', help='path to the version header file')
    parser.add_argument('--target', default='master', help='target branch')
    args = parser.parse_args()
    version_header = args.version
    target_branch = args.target
    subprocess.check_call(['git', 'fetch', '--depth=1', 'origin', target_branch, f':{target_branch}'])

    update_version(target_branch, version_header)
