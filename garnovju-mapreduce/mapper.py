#!/usr/bin/env python3.6

import sys
import io
import uuid
import random

for line in sys.stdin:
    try:
        id = line.strip()
        id = '{}/{}'.format(str(random.randint(1, 2 ** 64)), id)
        print(id)
    except ValueError as e:
        continue


