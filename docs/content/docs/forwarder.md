+++
title = "Transactional messages publishing"
description = "Publishing messages in transactional way with help of Forwarder component"
date = 2021-01-13T12:47:30+01:00
weight = -400
draft = false
bref = "Publishing messages in transactions"
toc = true
+++

## Publishing messages in transactions (and why we should care) 
While working with a system that operates on data stored in one 


### Publishing event first, storing data next
{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/real-world-examples/transactional-events-forwarder/main.go" first_line_contains="// 1. Publishes event" last_line_contains="// In case this fails" padding_after="9" %}}
{{% /render-md %}}

### Storing data first, publishing event next
{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/real-world-examples/transactional-events-forwarder/main.go" first_line_contains="// 2. Persists data" last_line_contains="// In case this fails" padding_after="9" %}}
{{% /render-md %}}

### Storing data and publishing event in one transaction
{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/real-world-examples/transactional-events-forwarder/main.go" first_line_contains="// 3. Persists data" last_line_contains="err = publisher.Publish(googleCloudEventTopic" padding_after="5" %}}
{{% /render-md %}}
