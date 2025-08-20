+++
title = "Middleware"
description = "Add generic functionalities to your handlers in an unobtrusive way"
date = 2019-06-01T19:00:00+01:00
weight = -500
draft = false 
bref = "Add functionality to handlers"
+++

## Introduction

Middleware wrap handlers with functionality that is important, but not relevant for the primary handler's logic. 
Examples include retrying the handler after an error was returned, or recovering from panic in the handler
and capturing the stacktrace.

Middleware wrap the handler function like this:

{{% load-snippet-partial file="src-link/message/router.go" first_line_contains="// HandlerMiddleware" last_line_contains="type HandlerMiddleware" %}}

## Usage

Middleware can be executed for all as well as for a specific handler in a router. When middleware is added directly 
to a router it will be executed for all handlers provided for a router. If a middleware should be executed only 
for a specific handler, it needs to be added to handler in the router.

Example usage is shown below:

{{% load-snippet-partial file="src-link/_examples/basic/3-router/main.go" first_line_contains="router, err := message.NewRouter(message.RouterConfig{}, logger)" last_line_contains="// Now that all handlers are registered, we're running the Router." padding_after="1" %}}

## Available middleware

Below are the middleware provided by Watermill and ready to use. You can also easily implement your own.
For example, if you'd like to store every received message in some kind of log, it's the best way to do it.

{{% readfile file="/content/src-link/middleware-defs.md" %}}

