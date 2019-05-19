# configuration for the dev/validate_example script.
#
# how to run the command
CMD="docker-compose up"
# how long to wait for the output 
TIMEOUT=30
# we expect this regexp to be present in the output
EXPECTED_OUTPUT="received message: [0-9a-f\\-]+, payload: Hello, world!"
