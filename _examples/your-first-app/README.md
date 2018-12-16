# Your first app

Before checking the examples, it is recommended to read [getting started guide](https://watermill.io/docs/getting-started/).

## Files

- [main.go](main.go) - example source code, probably the **most interesting file to you**
- [docker-compose.yml](docker-compose.yml) - local environment Docker Compose configuration, contains Golang, Kafka and Zookeeper
- [go.mod](go.mod) - Go modules dependencies, you can find more information at [Go wiki](https://github.com/golang/go/wiki/Modules)
- [go.sum](go.sum) - Go modules checksums

## Requirements

To run this example you will need Docker and docker-compose installed. See installation guide at https://docs.docker.com/compose/install/

## Running

```bash
> docker-compose up
[a lot of Kafka logs...]
server_1     | 2018/11/18 11:16:34 received event 1542539794
server_1     | 2018/11/18 11:16:35 received event 1542539795
server_1     | 2018/11/18 11:16:36 received event 1542539796
server_1     | 2018/11/18 11:16:37 received event 1542539797
server_1     | 2018/11/18 11:16:38 received event 1542539798
server_1     | 2018/11/18 11:16:39 received event 1542539799
```

Now all that's left is to take a look at the Kafka topics to check that all messages are there:

```bash
> docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic deadly-easy-topic

{"num":1542539605}
{"num":1542539606}
{"num":1542539607}
{"num":1542539608}
{"num":1542539609}
{"num":1542539610}
```

```bash
> docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic deadly-easy-topic_processed

{"event_num":1542539642,"time":"2018-11-18T11:14:02.26452706Z"}
{"event_num":1542539643,"time":"2018-11-18T11:14:03.26633757Z"}
{"event_num":1542539644,"time":"2018-11-18T11:14:04.268420818Z"}
{"event_num":1542539645,"time":"2018-11-18T11:14:05.270183092Z"}
{"event_num":1542539646,"time":"2018-11-18T11:14:06.272387936Z"}
{"event_num":1542539647,"time":"2018-11-18T11:14:07.274663833Z"}
