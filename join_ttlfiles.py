import time
from datetime import timedelta
import re
import shutil
import io
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('--input_directory', required=True, type=str, help='location of ttl files')
parser.add_argument('--replacement_pairs', nargs='+', type=str, help='list of prefixes to replace. Input as prefix=replacement')
parser.add_argument('--output_file', type=str, required=True, help='Target file for storing resulting ttl files.')
args = parser.parse_args()


start = time.time()
print(f'Joining files and writing to {args.output_file}')

# parse replacements if present
replacements = {}
for pair in args.replacement_pairs:
    prefix, uri = pair.split('=')
    replacements.update({prefix: uri})

# read file names
files = []
for s in os.listdir(args.input_directory):
    if re.match(r'.*.ttl$', s):
        files.append(os.path.join(args.input_directory, s))

prefixes = []
prefix_ends = {}

# read unique prefixes and find end of prefix enumeration
print('- reading prefixes')
for f in files:
    with open(f, 'r') as file:
        line = next(file)
        pos = 0
        while re.match(r'^@prefix.*', line):
            if line.strip() not in prefixes:
                prefixes.append(line.strip())
            pos += 1
            try:
                line = next(file)
            except StopIteration:
                break
    prefix_ends.update({f: pos})

if replacements:
    print('- replacing specified prefixes')
    for pos, prefix in enumerate(prefixes):
        parts = prefix.split(' ')
        if parts[1][:-1] in replacements:
            parts[2] = f'<{replacements[parts[1][:-1]]}>'
            prefixes[pos] = ' '.join(parts)

# do file writing with copyfileobj
# testing showed using this method halves the runtime
with open(args.output_file, 'wb') as target:
    # write list of prefixes
    print(f'- writing prefixes')
    for prefix in prefixes:
        prefix += '\n'
        b = prefix.encode()
        p_encoded = io.BytesIO(b)
        shutil.copyfileobj(p_encoded, target)
    # concatenate data
    for pos, f in enumerate(files):
        print(f'- concatenating {f}')
        start_line = prefix_ends[f]
        with open(f, 'rb') as source:
            lines = source.readlines()
            b = io.BytesIO(b''.join(lines[start_line:]))
            shutil.copyfileobj(b, target)


end = time.time()
print(f"Total runtime: {timedelta(seconds=end - start)}")
