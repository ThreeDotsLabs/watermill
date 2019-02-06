# configuration for the dev/validate_example script.
#
# how to run the command
CMD="docker-compose up"
# how long to wait for the output 
TIMEOUT=10
# we expect this regexp to be present in the output
EXPECTED_OUTPUT="msg=\"Message sent to Kafka\""
