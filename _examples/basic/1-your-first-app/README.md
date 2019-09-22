# Your first Watermill app

This example project shows a basic setup of Watermill. The application runs in a loop, consuming events from a Kafka
topic, modyfing them and publishing to another topic.

There's a docker-compose file included, so you can run the example and see it in action.

To understand the background and internals, see [getting started guide](https://watermill.io/docs/getting-started/).

## Files

- [main.go](main.go) - example source code, the **most interesting file for you**
- [docker-compose.yml](docker-compose.yml) - local environment Docker Compose configuration, contains Golang, Kafka and Zookeeper
- [go.mod](go.mod) - Go modules dependencies, you can find more information at [Go wiki](https://github.com/golang/go/wiki/Modules)
- [go.sum](go.sum) - Go modules checksums

## Requirements

To run this example you will need Docker and docker-compose installed. See the [installation guide](https://docs.docker.com/compose/install/).

## Running

```bash
> docker-compose up
[some initial logs]
server_1     | 2019/08/29 19:41:23 received event {ID:0}
server_1     | 2019/08/29 19:41:23 received event {ID:1}
server_1     | 2019/08/29 19:41:23 received event {ID:2}
server_1     | 2019/08/29 19:41:23 received event {ID:3}
server_1     | 2019/08/29 19:41:24 received event {ID:4}
server_1     | 2019/08/29 19:41:25 received event {ID:5}
server_1     | 2019/08/29 19:41:26 received event {ID:6}
server_1     | 2019/08/29 19:41:27 received event {ID:7}
server_1     | 2019/08/29 19:41:28 received event {ID:8}
server_1     | 2019/08/29 19:41:29 received event {ID:9}
```

Open another termial and take a look at Kafka topics to see that all messages are there. The initial events should be present on the `events` topic:

```bash
> docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic events

{"id":12}
{"id":13}
{"id":14}
{"id":15}
{"id":16}
{"id":17}
```

And the processed messages will be stored in the `events-processed` topic:

```bash
> docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic events-processed 

{"processed_id":21,"time":"2019-08-29T19:42:31.4464598Z"}
{"processed_id":22,"time":"2019-08-29T19:42:32.4501767Z"}
{"processed_id":23,"time":"2019-08-29T19:42:33.4530692Z"}
{"processed_id":24,"time":"2019-08-29T19:42:34.4561694Z"}
{"processed_id":25,"time":"2019-08-29T19:42:35.4608918Z"}
```
