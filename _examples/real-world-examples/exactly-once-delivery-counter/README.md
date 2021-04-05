# Exactly-once delivery counter

Is exactly-once delivery impossible? Well, it depends a lot on the definition of exactly-once delivery.
When we will assume that we want to avoid the situation when a message is delivered more than once when our broker or worker died -- it's possible.
I'll say more, it's even possible with Watermill!

![](./at-least-once-delivery.jpg)  
*At-least once delivery*

They are just two constraints:
1. you need to use Pub/Sub implementation that does support exactly-once delivery (only [MySQL/PostgreSQL](https://github.com/ThreeDotsLabs/watermill-sql) for now),
2. writes need to go to the same DB.

In practice, our model is pretty similar to how does it work with Kafka exactly-once delivery. If you want to know more details, you can check [their article](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/).

In our example, we will use MySQL database to implement a **simple counter**. It can be triggered by calling `http://localhost:8080/count/{counterUUID}` endpoint.
Calling this endpoint will publish a message to MySQL via our [Pub/Sub implementation](https://github.com/ThreeDotsLabs/watermill-sql).
The endpoint is provided by [server/main.go](server/main.go).

Later, the message is consumed by [worker/main.go](worker/main.go). The only responsibility of the worker is to update the counter in the MySQL database.
**Counter update is done in the same transaction as message consumption.**
Thanks to that fact and [A.C.I.D](https://en.wikipedia.org/wiki/ACID) even if server, worker or network failure due to the processing our data will stay consistent.


![](./architecture.jpg)
*Exactly-once delivery*

## Running

    docker-compose up

    go run run.go

*Please note, that `run.go` needs to be executed from the user having privileges to manage Docker.
It's due the fact that `run.go` is restarting containers.*
