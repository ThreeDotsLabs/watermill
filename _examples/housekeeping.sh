#!/bin/bash

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

# update_gomod finds all the go.mod enabled subprojects and updates their dependency
# to a specified version.
function update_gomod() {
        dependency="$1"
        if [ -z "$dependency" ]; then dependency="github.com/ThreeDotsLabs/watermill"; fi
        revision="$2"
        if [ -z "$revision" ]; then revision="master"; fi

        echo "Setting $dependency to $revision..."

        tput setaf 4 # blue
        for gomod in $(find . -name "go.mod")
        do
                dir="$(realpath --relative-to=$(pwd) $(dirname "$gomod"))"
                pushd "$(realpath $(dirname "$gomod"))" &> /dev/null

                # skip files that currently have no dependency
                if grep -q "$dependency" ./go.mod
                then
                        enumerate $dir
                        go get "$dependency@$revision" 2> /dev/null
                fi

                popd &> /dev/null
        done

        echo "...done"
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

        echo -e "\nChecking examples"

        enumerate "checking if kafka-to-http runs and has expected output"
        cd kafka-to-http
        check_example "docker-compose up" 20 "POST /foo_or_bar: message" || anyError=1
        cd ..

        return $anyError
}

function check_example() {
        RESULT=$(check_output "$@")

        if [[ "$?" -eq 0 ]]
        then
                pass
        else
                echo -e "\e[91m$RESULT\e[39m" # light red
                fail
                anyError=1
        fi
        cd ..

        return $anyError
}

if [ "$0" = "$BASH_SOURCE" ]
then
        # script executed directly, not sourced
        update_gomod
        check_examples
        exit $?
fi

