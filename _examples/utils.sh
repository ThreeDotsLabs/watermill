#!/usr/bin/env bash


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
        set +e
        echo "$(pwd)"
        echo "timeout $timeout "$cmd" &> "$output""
        timeout $timeout $cmd &> $output
        exitCode=$?
        set -e

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

