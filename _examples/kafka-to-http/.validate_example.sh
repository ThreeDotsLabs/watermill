# configuration for the dev/validate_example script.
#
# how to run the command
CMD="docker-compose up"
# how long to wait for the output 
TIMEOUT=30
# we expect this regexp to be present in the output
EXPECTED_OUTPUT="POST /foo_or_bar: message"
