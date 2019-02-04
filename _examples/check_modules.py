#!/bin/bash

# 1. Check that all the go.mod files have their watermill version pinned to `master`.
# Using other revisions might be useful for development, but when running this script in CI on `master`,
# we expect the examples to be pinned to the latest `master` revision.
import glob
import sys

PROJECT_URL = 'github.com/ThreeDotsLabs/watermill'


def check_modfiles():
    anyError = False
    modfiles = glob.glob('**/go.mod')
    for f in modfiles:
        version = get_modfile_version(f)
        if not version == 'master':
            print('{}: watermill version {}, expecting `master`'.format(f, version))
            anyError = True

    if anyError:
        sys.exit(1)


def get_modfile_version(path: str) -> str:
    for l in open(path):
        if PROJECT_URL in l:
            # expecting version to be in the second field of go.mod
            return l.split()[1]


if __name__ == '__main__':
    check_modfiles()
