# configuration for the dev/validate_example script.
#
# how to run the command
CMD="go run main.go"
# how long to wait for the output 
TIMEOUT=3
# we expect this regexp to be present in the output
EXPECTED_OUTPUT="structHandler received message [0-9a-f\\-]+"
