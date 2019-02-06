#!/bin/bash

# DISCLAIMER
#
# The timeouts in this file are arbitrary and may not be reliable.
# Thus, it is not suitable for CI, rather to be run locally by developers 
# to ensure that their changes didn't break existing examples/snippets.
#
# If any code check fails, investigate the log to find out about the cause.

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

        echo "Pinning $dependency to current $revision..."

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

        echo -e "\nChecking examples..."
        pushd _examples &> /dev/null

        pushd kafka-to-http &> /dev/null
        enumerate "checking if kafka-to-http runs and has expected output"
        wrap_check_output "docker-compose up" 30 "POST /foo_or_bar: message" || anyError=1
        popd &> /dev/null

        pushd simple-app &> /dev/null
        enumerate "checking simple-app/subscribing"
        wrap_check_output "docker-compose up" 10 "msg=\"Starting handler\"" || anyError=1
        enumerate "checking simple-app/publishing"
        wrap_check_output "docker-compose up" 10 "msg=\"Message sent to Kafka\"" || anyError=1
        popd &> /dev/null

        pushd your-first-app &> /dev/null
        enumerate "checking if your-first-app runs and has expected output"
        wrap_check_output "docker-compose up" 20 "received event [0-9]+" || anyError=1
        popd &> /dev/null

        echo "...done"
        popd &> /dev/null
        return $anyError
}

function check_docs_snippets() {
        anyError=0

        echo -e "\nChecking docs snippets..."
        pushd docs/content/docs &> /dev/null

        pushd snippets/amqp-consumer-groups &> /dev/null
        enumerate "checking amqp-consumer-groups snippet"
        wrap_check_output "docker-compose up" 10 " received message: [0-9a-f\\-]+, payload: Hello, world!" || anyError=1
        popd &> /dev/null

        pushd message &> /dev/null
        enumerate "checking the receiving-ack snippet"
        wrap_check_output "go run receiving-ack.go" 1 "ack received" || anyError=1
        popd &> /dev/null

        pushd getting-started/router &> /dev/null
        enumerate "checking the router getting-started snippet"
        wrap_check_output "go run main.go" 3 "structHandler received message [0-9a-f\\-]+" || anyError=1
        popd &> /dev/null

        pushd getting-started/kafka &> /dev/null
        enumerate "checking the kafka getting-started snippet"
        wrap_check_output "docker-compose up" 30 "payload: Hello, world!" || anyError=1
        popd &> /dev/null

        pushd getting-started/nats-streaming &> /dev/null
        enumerate "checking the nats getting-started snippet"
        wrap_check_output "docker-compose up" 5 "payload: Hello, world!" || anyError=1
        popd &> /dev/null

        pushd getting-started/googlecloud &> /dev/null
        enumerate "checking the google cloud pubsub getting-started snippet"
        wrap_check_output "docker-compose up" 5 "payload: Hello, world!" || anyError=1
        popd &> /dev/null

        pushd getting-started/amqp &> /dev/null
        enumerate "checking the rabbitmq getting-started snippet"
        wrap_check_output "docker-compose up" 10 "payload: Hello, world!" || anyError=1
        popd &> /dev/null

        pushd getting-started/go-channel &> /dev/null
        enumerate "checking the go channel pubsub getting-started snippet"
        wrap_check_output "go run main.go" 1 "payload: Hello, world!" || anyError=1
        popd &> /dev/null
        

        echo "...done"
        popd &> /dev/null
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
        pushd _examples &> /dev/null
        update_gomod
        popd &> /dev/null

        pushd docs &> /dev/null
        update_gomod
        popd &> /dev/null

        anyError=0

        check_examples || anyError=1
        check_docs_snippets || anyError=1

        exit $anyError
fi

