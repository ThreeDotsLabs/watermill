#!/usr/bin/env bats

@test "Check if kafka-to-http runs and returns expected output" {
        source ../utils.sh
        check_output "docker-compose up" 20 "POST /foo_or_bar: message"
}
