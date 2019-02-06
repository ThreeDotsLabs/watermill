# configuration for the dev/validate_example script.
#
# how to run the command
CMD="go run receiving-ack.go"
# how long to wait for the output 
TIMEOUT=1
# we expect this regexp to be present in the output
EXPECTED_OUTPUT="ack received"
