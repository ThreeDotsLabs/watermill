+++
title = "Getting started"
description = "Up and running in under a minute"
weight = -9999
draft = false
toc = true
bref = "todo"
type = "docs"
+++

### What is Watermill?

### Install

```bash
go get -u github.com/ThreeDotsLabs/watermill/
```

### Subscribing for messages

One of the most important parts of the Watermill is [*Message*]({{< ref "/docs/message" >}}). It is as important as `http.Request` for `http` package.
Almost every part of Watermill use this type in some part.

When we are building reactive/event-driven application/[insert your buzzword here] we always want to listen of incomming messages to react for them.
Watermill is supporting multiple [publishers and subscribers implementations]({{< ref "docs/pub-sub-implementations" >}}), with compatible interface and abstraction which provide similar behaviour.

Lets start with subscribing for messages.

{{% tabs id="subscribing" tabs="go-channel,kafka" labels="Go Channel,Kafka" %}}

{{% tabs-tab id="go-channel"%}}
{{% load-snippet-partial file="content/docs/getting-started/go-channel/main.go" first_line_contains="import (" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="content/docs/getting-started/go-channel/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="kafka" %}}
{{% message type="success" %}}
Installed `librdkafka` is required to run Kafka subscriber.
{{% /message %}}

{{< collapse id="installing_rdkafka" >}}

{{< collapse-toggle box_id="docker" >}}
Running in Docker
{{% /collapse-toggle %}}
{{% collapse-box id="docker" %}}
Easiest way to run Watermill with Kafka locally is using Docker.

{{% load-snippet file="content/docs/getting-started/kafka/docker-compose.yml" type="yaml" %}}

The source should go to `main.go`.

To run please executute `docker-compose up` command.

More detailed explonation of how it is running, and how to add live reload you can find in [our [...] article](todo).

{{% /collapse-box %}}
{{< collapse-toggle box_id="ubuntu" >}}
Installing librdkafka on Ubuntu
{{% /collapse-toggle %}}
{{% collapse-box id="ubuntu" %}}
Newest version of the `librdkafka` for Ubuntu distributions you can find in [Confluent](https://www.confluent.io/)'s repository.

```bash
# install `software-properties-common`, `wget`, or `gnupg` if not installed yet
sudo apt-get install -y software-properties-common wget gnupg

# add a new repository
wget -qO - https://packages.confluent.io/deb/4.1/archive.key | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/4.1 stable main"

# and then you can install newest version of `librdkafka`
sudo apt-get update && sudo apt-get -y install librdkafka1 librdkafka-dev
```
{{% /collapse-box %}}
{{< collapse-toggle box_id="redhat" >}}
Installing librdkafka on CentOS/RedHat/Fedora
{{% /collapse-toggle %}}
{{% collapse-box id="redhat" %}}
We will use [Confluent](https://www.confluent.io/)'s repository to download newest version of `librdkafka`.

```bash
# install `curl` and `which` if not already installed
sudo yum -y install curl which

# install Confluent public key
sudo rpm --import https://packages.confluent.io/rpm/4.1/archive.key

# add repository to /etc/yum.repos.d/confluent.repo
sudo cat > /etc/yum.repos.d/confluent.repo << EOF
[Confluent.dist]
name=Confluent repository (dist)
baseurl=https://packages.confluent.io/rpm/4.1/7
gpgcheck=1
gpgkey=https://packages.confluent.io/rpm/4.1/archive.key
enabled=1

[Confluent]
name=Confluent repository
baseurl=https://packages.confluent.io/rpm/4.1
gpgcheck=1
gpgkey=https://packages.confluent.io/rpm/4.1/archive.key
enabled=1
EOF

# clean YUM cache
sudo yum clean all

# install librdkafka
sudo yum -y install librdkafka1 librdkafka-dev
```

{{% /collapse-box %}}
{{< collapse-toggle box_id="osx" >}}
Installing librdkafka on OSX
{{% /collapse-toggle %}}
{{% collapse-box id="osx" %}}
todo
{{% /collapse-box %}}
{{< collapse-toggle box_id="building" >}}
Building from sources (for other distros)
{{% /collapse-toggle %}}
{{% collapse-box id="building" %}}
Manually compiling from sources:

```bash
wget -O "librdkafka.tar.gz" "https://github.com/edenhill/librdkafka/archive/v0.11.6.tar.gz"

mkdir -p librdkafka
tar --extract --file "librdkafka.tar.gz" --directory "librdkafka" --strip-components 1
cd "librdkafka"

./configure --prefix=/usr && make -j "$(getconf _NPROCESSORS_ONLN)" && make install
```

{{% /collapse-box %}}
{{< /collapse >}}

{{% load-snippet-partial file="content/docs/getting-started/kafka/main.go" first_line_contains="import (" last_line_contains="process(messages)" %}}
{{% load-snippet-partial file="content/docs/getting-started/kafka/main.go" first_line_contains="func process" %}}
{{% /tabs-tab %}}

{{% /tabs %}}

### Publishing messages

{{% tabs id="publishing" tabs="go-channel,kafka" labels="Go Channel,Kafka" %}}

{{% tabs-tab id="go-channel"%}}
{{% load-snippet-partial file="content/docs/getting-started/go-channel/main.go" first_line_contains="go process(messages)" last_line_contains="publisher.Publish" padding_after="4" %}}
{{% /tabs-tab %}}

{{% tabs-tab id="kafka" %}}
{{% load-snippet-partial file="content/docs/getting-started/kafka/main.go" first_line_contains="go process(messages)" last_line_contains="publisher.Publish" padding_after="4" %}}
{{% /tabs-tab %}}

{{% /tabs %}}


### Using *Messages Router*

[*Publishers and subscribers*]({{< ref "docs/pub-sub" >}}) are rather low-level parts of Watermill.
In production use we want usually use something which is higher level and provides some features like [correlation, metrics, poison queue, retrying, throttling etc]({{< ref "/docs/messages-router-middleware" >}}).

We also don't want to manually send Ack when processing was successful. Sometimes, we also want send a message after processing another.

To handle these requirements we created component named [*Router*]({{< ref "docs/messages-router" >}}). In this example, we also will implement more common business case: sending order confirmation and requesting shipping.

#### Router configuration

For the beginning we should start with configuration of the router. We will configure which plugins and middlewares we want to use.

We also will set up handlers which this router will support. Every handler will independently handle the messages.

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/router/main.go" first_line_contains="package" last_line_contains="router.Run()" padding_after="4" %}}
{{% /render-md %}}

#### Producing events

We need some events, which may simulate work of real shop:

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/router/main.go" first_line_contains="type OrderPlaced struct {" last_line_contains="time.Sleep(time.Second)" padding_after="2" %}}
{{% /render-md %}}

#### Handlers

You may notice that we have two types of *handler functions*:

1. function `func(msg *message.Message) ([]*message.Message, error)`
2. method `func (c sendOrderPlacedEmail) Handler(msg *message.Message) ([]*message.Message, error)`

Second option is useful, when our function requires some dependencies like database, logger etc.
When we have just function without dependencies, it's fine to use just a function.

Let's take a look for `requestOrderShipping` handler:

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/router/main.go" first_line_contains="type requestOrderShipping struct {" last_line_contains="(ShippingRequested) EventName()" padding_after="3" %}}
{{% /render-md %}}

#### Event marshalers

Watermill doesn't enforce any event format - it's up to you how you will represent them. Here is one of the possible implementations:

{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/router/main.go" first_line_contains="type Event struct {" last_line_contains="return true, nil" padding_after="2" %}}
{{% /render-md %}}


#### Done!

We can just run this example by `go run main.go`.

We just created our first application with Watermill. The full source you can find in [/docs/getting-started/router/main.go](https://github.com/ThreeDotsLabs/watermill/tree/master/content/docs/getting-started/router/main.go).

### Deployment

Watermill is not a framework. We don't enforce any type of deployment and it's totally up to you.

### What's next?

For more detailed documentation you should check [documentation topics list]({{< ref "/docs" >}}).

If anything is not clear feel free to use any of our [support channels]({{< ref "/support" >}}), we will we'll be glad to help.