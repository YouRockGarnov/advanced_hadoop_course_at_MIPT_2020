#!/usr/bin/env python3.6

import sys
import io
import uuid


input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
output_stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

for line in input_stream:
    try:
        id = line.strip()
    except ValueError as e:
        continue

    id = str(uuid.uuid4()) + '/' + id
    print(f"{id}", file=output_stream)
