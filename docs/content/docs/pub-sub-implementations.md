+++
title = "Pub/Sub's implementations"
description = ""
date = 2018-12-05T12:47:48+01:00
weight = -800
draft = false
bref = ""
toc = false
+++

| Name | Publisher | Subscriber | Status |
|------|-----------|------------|--------|
|  [Golang Channel](#golang-channel) | x | x | `prod-ready` |
|  [Kafka](#kafka) | x | x | `prod-ready` |
|  [HTTP](#http)  |   | x | `prod-ready` |
|  [Google Cloud Pub/Sub](#google-cloud-pub-sub)  | x | x | [`in-development`](https://github.com/ThreeDotsLabs/watermill/pull/10) |
|  MySQL Binlog  |  | x | [`idea`](https://github.com/ThreeDotsLabs/watermill/issues/5) |

All built-in implementations can be found in [`message/infrastructure`](https://github.com/ThreeDotsLabs/watermill/tree/master/message/infrastructure).

### Golang Channel

bla bla bla

##### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | no | |
| ExactlyOnceDelivery | yes |  |
| GuaranteedOrder | yes |  |
| Persistent | no| |


##### Configuration

### Kafka

bla bla bla

##### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | yes | |
| ExactlyOnceDelivery | no | in theory can be achieved with [Transactions](https://www.confluent.io/blog/transactions-apache-kafka/), currently no support for any Golang client  |
| GuaranteedOrder | yes | require [paritition key usage](#using-partition-key)  |
| Persistent | yes| |

##### Configuration

##### Using partition key

##### No consumer group

[config info]

### HTTP

bla bla bla

##### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | no | |
| ExactlyOnceDelivery | no |  |
| GuaranteedOrder | yes |  |
| Persistent | no| |

##### Configuration

### Google Cloud Pub/Sub

### Implementing your own Pub/Sub

[todo - link]