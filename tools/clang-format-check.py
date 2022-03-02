#!/usr/bin/env python
from __future__ import absolute_import, division, print_function

import argparse
import subprocess
import xml.dom.minidom

from collections import namedtuple
from io import BytesIO

Replacement = namedtuple('Repalcement', 'offset length')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('files', nargs='+',
                        help='path to the file(s) to check')
    parser.add_argument('--binary', default='clang-format',
                        help='location of binary to use for clang-format')
    args = parser.parse_args()

    for filename in args.files:
        command = [args.binary, filename, '--output-replacements-xml']
        p = subprocess.Popen(command,
                             stdout=subprocess.PIPE,
                             stderr=None,
                             stdin=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise SystemExit(p.returncode)

        if not stdout:
            print('info: nothing to do')
            raise SystemExit()

        replacements = []
        with BytesIO(stdout) as report:
            input_dom = xml.dom.minidom.parse(report)
            xml_repls = input_dom.getElementsByTagName('replacements')
            if not xml_repls:
                raise SystemExit('error: missing replacements element')
            xml_repls = xml_repls[0].getElementsByTagName('replacement')
            for replacement in xml_repls:
                offset = int(replacement.attributes['offset'].nodeValue)
                length = int(replacement.attributes['length'].nodeValue)
                replacements.append(Replacement(offset, length))

        if not replacements:
            print('info: nothing to do')
            raise SystemExit()

        # offset and length are byte offsets, so we need to read and operate on
        # bytes rather than decoded strings
        with open(filename, 'rb') as f:
            code = f.read()

        line = 1
        column = 1
        offset = 0

        for replacement in replacements:
            chunk = code[offset:replacement.offset]
            try:
                ridx = chunk.rindex(b'\n')
            except ValueError:
                column += len(chunk.decode('utf-8'))
            else:
                column = len(chunk[ridx + 1:].decode('utf-8')) + 1
                line += chunk[:ridx + 1].count(b'\n')

            # Only report the first line of the replacement
            print('{}:{}:{}: code formatting required'.format(
                      filename, line, column))

            chunk = code[replacement.offset:replacement.length]
            try:
                ridx = chunk.rindex(b'\n')
            except ValueError:
                column += len(chunk.decode('utf-8'))
            else:
                column = len(chunk[ridx + 1:].decode('utf-8')) + 1
                line += chunk[:ridx + 1].count(b'\n')

            offset = replacement.offset + replacement.length


if __name__ == '__main__':
    main()
