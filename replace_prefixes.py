import argparse
import re
import shutil
import io
import os

def replace_and_save(replacements: dict, input_file: str, output_file:str) -> None:
    prefixes = []

    # read unique prefixes and find end of prefix enumeration
    print('- reading prefixes')
    with open(input_file, 'r') as file:
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

    prefix_ends = pos

    for pos, prefix in enumerate(prefixes):
        parts = prefix.split(' ')
        if parts[1][:-1] in replacements:
            parts[2] = f'<{replacements[parts[1][:-1]]}>'
            prefixes[pos] = ' '.join(parts)

    with open(output_file, 'wb') as target:
        # write list of prefixes
        print(f'- writing prefixes')
        for prefix in prefixes:
            prefix += '\n'
            b = prefix.encode()
            p_encoded = io.BytesIO(b)
            shutil.copyfileobj(p_encoded, target)
        # concatenate data
        start_line = prefix_ends
        with open(input_file, 'rb') as source:
            lines = source.readlines()
            b = io.BytesIO(b''.join(lines[start_line:]))
            shutil.copyfileobj(b, target)


def main() -> None:
    parser = argparse.ArgumentParser()
    input_opt = parser.add_mutually_exclusive_group(required=True)
    input_opt.add_argument('--input_file', type=str, help='input file to replace prefix in')
    input_opt.add_argument('--input_dir', type=str, help='input directory to replace prefixes in')
    output_opt = parser.add_mutually_exclusive_group(required=True)
    output_opt.add_argument('--output_file', type=str, help='name of outputfile to write to')
    output_opt.add_argument('--output_dir', type=str, help='name of output directory to write to')
    parser.add_argument('--replacement_pairs', required=True, nargs='+', type=str, help='list of prefixes to replace. Input as prefix=replacement')
    args = parser.parse_args()

    # parse replacements
    replacements = {}
    for pair in args.replacement_pairs:
        prefix, uri = pair.split('=')
        replacements.update({prefix: uri})

    # run renaming
    if args.input_file and args.output_file:
        replace_and_save(replacements, args.input_file, args.output_file)

    elif args.input_dir and args.output_dir:
        # create output directory
        if not os.path.exists(args.output_dir):
            os.mkdir(args.output_dir)

        # loop input directory
        for s in os.listdir(args.input_dir):
            if re.match(r'.*.ttl$', s):
                print(f'processing: {s}')
                replace_and_save(replacements, os.path.join(args.input_dir, s), os.path.join(args.output_dir, s))

    else:
        raise Exception('input and output are not set or types do not match')


if __name__ == '__main__':
    main()
