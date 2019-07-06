+++
title = "Pub/Sub implementations"
description = "Golang channel, Kafka, Google Cloud Pub/Sub, RabbitMQ and more!"
date = 2018-12-05T12:47:48+01:00
weight = -800
draft = true 
bref = "Golang channel, Kafka, Google Cloud Pub/Sub, RabbitMQ and more!"
toc = false
+++

| Name | Publisher | Subscriber | Status |
|------|-----------|------------|--------|
|  [Golang Channel]({{< ref "#golang-channel" >}}) | x | x | `prod-ready` |
|  [Kafka]({{< ref "#kafka" >}}) | x | x | `prod-ready` |
|  [HTTP]({{< ref "#http" >}})  | x | x | `prod-ready` |
|  [Google Cloud Pub/Sub]({{< ref "#google-cloud-pub-sub" >}})  | x | x | `prod-ready` |
|  [NATS Streaming]({{< ref "#nats-streaming" >}})  | x | x | `prod-ready` |
|  [RabbitMQ (AMQP)]({{< ref "#rabbitmq-amqp" >}})  | x | x | `prod-ready` |
|  [io.Writer/io.Reader]({{< ref "#io-writer-io-reader" >}})  | x | x | `experimental` |
|  MySQL Binlog  |  | x | [`idea`](https://github.com/ThreeDotsLabs/watermill/issues/5) |

All built-in implementations can be found in [message/infrastructure](https://github.com/ThreeDotsLabs/watermill/tree/master/message/infrastructure).


(Sections for each pub/sub would follow here, but were moved to pubsubs/)

