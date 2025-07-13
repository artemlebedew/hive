#! /usr/bin/env python3

import sys
import re

searching_regex = r'((?:ht|f)tps?://[^/]+\.)([a-zA-Z]{2,}/)'
replacing_regex = r'\1com/'

for line in sys.stdin:
    row = line.strip().split('\t')
    row[2] = re.sub(searching_regex, replacing_regex, row[2])
    print("\t".join(row))