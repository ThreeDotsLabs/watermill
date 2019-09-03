#!/usr/bin/python3
import os
import re


class MiddlewareSourceFile:
    def __init__(self, filepath: str):
        self._definitions = []
        self._filepath = filepath
        with open(filepath, 'r') as f:
            self._src = f.readlines()
            self.name = os.path.basename(self._filepath)

        for (line_no, line) in enumerate(self._src):
            # function or struct definitions
            if line.startswith('func') or re.match(r'^type \S+? struct', line):
                # with godocs
                if line_no > 0 and (self._src[line_no - 1]).startswith('//'):
                    self.add_func_with_godoc(line_no - 1)

    def add_func_with_godoc(self, line_no: int):
        func_def = ''

        # find the first line of godoc
        while True:
            if line_no - 1 > 0 and self._src[line_no - 1].startswith('//'):
                line_no -= 1
            else:
                break

        while True:
            line_content = self._src[line_no]
            func_def += line_content
            line_no += 1

            # go fmt, I believe in you
            if line_content == '}\n':
                break

        self._definitions.append(func_def)

    def format(self) -> str:
        if not self._definitions:
            return None

        middleware_name = self.name.strip('.go').replace('_', ' ')
        middleware_name = capitalize(middleware_name)
        s = '### {}\n\n'.format(middleware_name)

        for func_def in self._definitions:
            s += '```go\n{}```\n'.format(func_def)

        return s


def capitalize(s: str) -> str:
    words = s.split()
    for i, word in enumerate(words):
        words[i] = word[0].upper() + word[1:]

    return ' '.join(words)


if __name__ == '__main__':
    go_sources = []
    for root, dirs, files in os.walk('../message/router/middleware'):
        go_sources = [MiddlewareSourceFile(os.path.join(root, f)) for f in files if f.endswith('.go') and not f.endswith('_test.go')]

    for src_file in go_sources:
        formatted = src_file.format()
        if formatted:
            print(formatted)
            print('\n')

    pass
