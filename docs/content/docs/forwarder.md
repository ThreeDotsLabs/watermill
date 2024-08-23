+++
title = "Forwarder component"
description = "Implement outbox pattern by publishing messages in transactional way"
date = 2021-01-13T12:47:30+01:00
weight = -400
draft = false
bref = "Emitting events along with storing data in a database in one transaction"
toc = true
+++

## Publishing messages in transactions (and why we should care) 
While working with an event-driven application, you may in some point need to store an application state and publish a message 
telling the rest of the system about what's just happened. In a perfect scenario, you'd want to persist the application state 
and publish the message **in a transaction**, as not doing so might get you easily into troubles with data consistency. In 
order to commit both storing data and emitting an event in one transaction, you'd have to be able to publish 
messages to the same database you use for the data storage, 
or implement [2PC](https://martinfowler.com/articles/patterns-of-distributed-systems/two-phase-commit.html) 
on your own. If you don't want to change your message broker to a database, nor invent the wheel once again,
you can make your life easier by using Watermill's [Forwarder component](https://github.com/ThreeDotsLabs/watermill/blob/master/components/forwarder/forwarder.go)! 

## Forwarder component 
You can think of the Forwarder as a background running daemon which awaits messages that are published to a database, and makes sure they eventually reach a message broker.  

<img src="/img/publishing-with-forwarder.svg" alt="Watermill Forwarder component" style="width:100%;" />

In order to make the Forwarder universal and usable transparently, it listens to a single topic on an intermediate 
database based Pub/Sub, where enveloped messages are sent with help of a decorated [Forwarder Publisher](https://github.com/ThreeDotsLabs/watermill/blob/9e04bfefbd6fef9f9ffa59956654277005fa2e8a/components/forwarder/publisher.go#L30). 
The Forwarder unwraps them, and sends to a specified destined topic on the message broker.  

<img src="/img/forwarder-envelope.svg" alt="Forwarder envelope" style="width:100%;" />

## Example

Let's consider a following example: there's a command which responsibility is to run a lottery. It has to pick 
a random user that's registered in the system as a winner. While it does so, it should also persist the decision it made by 
storing a database entry associating a unique lottery ID with a picked user's ID. Additionally, as it's an 
event-driven system, it should emit a `LotteryConcluded` event, so that other components could react to that appropriately. 
To be precise - there will be component responsible for sending prizes to lottery winners. It will receive `LotteryConcluded`
events, and using the lottery ID embedded in the event, verify who was the winner, checking with the database entry. 

In our case, the database is MySQL and the message broker is Google Pub/Sub, but it could be any two other technologies.  

Approaching to implementation of such a command, we could go various ways. Below we're going to cover three possible 
attempts, pointing their vulnerabilities. 

### Publishing an event first, storing data next
In this approach, the command is going to publish an event first and store data just after that. While in most of the 
cases that approach will probably work just fine, let's try to find out what could possibly go wrong. 

There are three basic actions that the command has to do:

1. Pick a random user `A` as a lottery winner.
2. Publish a `LotteryConcluded` event telling that lottery `B` has been concluded. 
3. Store in the database that the lottery `B` has been won by the user `A`. 

Every of these steps could potentially fail, breaking the flow of our command. The first point wouldn't have huge 
repercussions in case of its failure - we would just return an error and consider the whole command failed. No data would
be stored, no event would be emitted. We can simply rerun the command. 

In case the second point fails, we'll still have no event emitted and no data stored in the database. We can rerun the 
command and try once again. 

What's most interesting is what could happen in case the third point fails. We'd already have the event emitted after 
the second point, but no data would be stored eventually in the database. Other components would get a signal 
that the lottery had been concluded, but no winner would be associated to the lottery ID sent in the event. They wouldn't 
be able to verify who's the winner, so their action would have to be considered failed as well. 

We still can get out of this situation, but most probably it will require some manual action, i.e., rerunning the command 
with the lottery ID that the emitted event has.

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/real-world-examples/transactional-events-forwarder/main.go" first_line_contains="// 1. Publishes event" last_line_contains="// In case this fails" padding_after="9" %}}
{{% /render-md %}}

### Storing data first, publishing an event next
In the second approach, we're going to try address first approach's drawbacks. We won't leak our failure to outer 
components by not emitting an event in case we don't have the state persisted properly in database. That means 
we'll change the order of our actions to following:

1. Pick a random user `A` as a lottery winner.
2. Store in the database that the lottery `B` has been won by the user `A`.
3. Publish a `LotteryConcluded` event telling that lottery `B` has been concluded.

Having two first actions failed, we have no repercussions, just as in the first approach. In case of failure of the 3rd 
point, we'd have data persisted in the database, but no event emitted. In this case, we wouldn't leak our failure 
outside the lottery component. Although, considering the expected system behavior, we'd have no prize sent to our winner,
because no event would be delivered to the component responsible for this action. 

That probably can be fixed by some manual action as well, i.e., emitting the event manually. We still can do better. 

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/real-world-examples/transactional-events-forwarder/main.go" first_line_contains="// 2. Persists data" last_line_contains="// In case this fails" padding_after="9" %}}
{{% /render-md %}}

### Storing data and publishing an event in one transaction
Let's imagine our command could do the 2nd, and the 3rd point at the same time. They would be committed atomically, 
meaning that any of them can't succeed having the other failed. This can be achieved by leveraging a transaction
mechanism which happens to be implemented by most of the databases in today's world. One of them is MySQL used in our 
example. 

In order to commit both storing data and emitting an event in one transaction, we'd have to be able to publish our 
messages to MySQL. Because we don't want to change our message broker to be backed by MySQL in the whole system, 
we have to find a way to do that differently. 

There's a good news: Watermill provides all the tools straight away! In case the database you're using is one among MySQL,
PostgreSQL (or any other SQL), Firestore or Bolt, you can publish messages to them. **Forwarder** component will help 
you with picking all the messages you publish to the database and forwarding them to a message broker of yours. 

Everything you have to do is to make sure that:

1. Your command uses a publisher working in a context of a database transaction (i.e. [SQL](https://github.com/ThreeDotsLabs/watermill-sql/blob/4f39bf82b6180ca2191c791e7cb220fff22b9255/pkg/sql/publisher.go#L53), 
[Firestore](https://github.com/ThreeDotsLabs/watermill-firestore/blob/b7bd31b3458884dc76076196cdc8942d18b5ab61/pkg/firestore/transactional.go#L14), [Bolt](https://github.com/ThreeDotsLabs/watermill-bolt/blob/0652f3602f6adbe4e3e39b97308fbed16dcbe29e/pkg/bolt/tx_publisher.go#L24)).
2. **Forwarder** component is running, using a database subscriber, and a message broker publisher.  

The command could look like following in this case:

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/real-world-examples/transactional-events-forwarder/main.go" first_line_contains="// 3. Persists data" last_line_contains="err = publisher.Publish(googleCloudEventTopic" padding_after="5" %}}
{{% /render-md %}}

In order to make the **Forwarder** component work in background for you and forward messages from MySQL to Google Pub/Sub,
you'd have to set it up as follows:

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/real-world-examples/transactional-events-forwarder/main.go" first_line_contains="// Setup the Forwarder " last_line_contains="err := fwd.Run" padding_after="3" %}}
{{% /render-md %}}

If you wish to explore the example more, you can find it implemented [here](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/real-world-examples/transactional-events-forwarder/main.go).
