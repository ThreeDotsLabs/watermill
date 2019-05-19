#!/bin/bash

# DISCLAIMER
#
# The timeouts in this file are arbitrary and may not be reliable.
# Thus, it is not suitable for CI, rather to be run locally by developers 
# to ensure that their changes didn't break existing examples/snippets.
#
# If any code check fails, investigate the log to find out about the cause.

set -e

function enumerate() {
        msg=$1
        # message in green
        echo -e "\e[32m\t✓ $msg\e[39m"
}

function pass() {
        # light green
        echo -e "\e[92m\tPASS\e[39m"
        tput sgr0
}

function fail() {
        # red
        echo -e "\e[31m\tFAIL\e[39m"
}

# check_output runs a selected command in the caller's working directory for a defined time;
# It looks in the logs for a defined phrase. The phrase may be a grep-compatible regexp.
#
# Returns 0 if the phrase is found before the timeout.
# Returns the error code from the command if it ended in an error.
# Returns 125 if the requested phrase was not found in output.
function check_output() {
        cmd=$1
        timeout=$2
        look_for=$3

        # prepare the log dump
        output=$(mktemp)

        # run the command and dump the logs
        echo "$(pwd)"
        echo "timeout $timeout "$cmd" &> "$output""
        timeout $timeout $cmd &> $output
        exitCode=$?

        if [ "$exitCode" -eq 124 ]
        then
                echo "Command timed out in ${timeout}s"
        elif [ "$exitCode" -ne 0 ]
        then
                echo "Command exited with error $exitCode"
                rm "$output"
                return $exitCode
        fi

        # check the logs for the defined phrase
        echo "Checking logs for $look_for"
        phrase="$(grep -E "$look_for" "$output")" || true

        if [ -z "$phrase" ]
        then
                echo "Phrase $look_for not found in output"
                echo "$output left for inspection"
                return 125
        else
                echo "Found phrase $look_for in output:"
                echo "$phrase"
                rm "$output"
                return
        fi
}

function check_examples() {
        anyError=0

        echo "Checking output of snippets and examples..."

        for conf in $(find . -name ".validate_example*.sh")
        do
                dir="$(dirname $conf)"
                # we expect $conf to supply $CMD, $TIMEOUT and $EXPECTED_OUTPUT
                source "$conf"
                pushd $dir &> /dev/null
                enumerate "$conf"
                wrap_check_output "$CMD" $TIMEOUT "$EXPECTED_OUTPUT"  || anyError=1
                popd &> /dev/null
                unset CMD && unset TIMEOUT && unset EXPECTED_OUTPUT
        done
        

        echo "...done"
        return $anyError
}

function wrap_check_output() {
        RESULT=$(check_output "$@")

        if [[ "$?" -eq 0 ]]
        then
                pass
        else
                echo -e "\e[91m$RESULT\e[39m" # light red
                fail
                anyError=1
        fi

        return $anyError
}

if [ "$0" = "$BASH_SOURCE" ]
then
# script executed directly, not sourced
        anyError=0

        check_examples || anyError=1
        exit $anyError
fi

