import argparse
import re

parser = argparse.ArgumentParser()
parser.add_argument('--input_file', required=True, type=str, help='input file to replace prefix in')
parser.add_argument('--replacement_pairs', required=True, nargs='+', type=str, help='list of prefixes to replace. Input as prefix=replacement')
parser.add_argument('--output_file', required=True, type=str, help='name of outputfile to write to')
args = parser.parse_args()

replacements = {}
for pair in args.replacement_pairs:
    prefix, uri = pair.split('=')
    replacements.update({prefix: uri})

with open(args.input_file, 'r', encoding='utf-8') as file:
    with open(args.output_file, 'w', encoding='utf-8') as output_file:
        found_prefix = False
        end_prefix = False
        prefixes = []
        for line in file.readlines():
            if not end_prefix:
                if re.match(r'^@prefix.*', line):
                    parts = line.split(' ')
                    if parts[1][:-1] in replacements:
                        parts[2] = f'<{replacements[parts[1][:-1]]}>'
                    line = ' '.join(parts)
                else:
                    if found_prefix:
                        end_prefix = True
            output_file.write(line)